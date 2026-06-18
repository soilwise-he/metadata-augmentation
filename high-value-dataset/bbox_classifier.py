"""Classify WGS 84 bounding boxes by their overlap with European countries.

Each bbox is labelled as exactly one of:

- ``single_country_eu`` — European bbox dominated by one country (may also
  contain parts of bordering countries).
- ``multi_country_eu`` — European bbox covering several countries, no dominator.
- ``non_european`` — bbox is genuinely about somewhere else, even if it
  accidentally clips a European border.
- ``invalid`` — parseable but is not usable WGS 84 (projected/metric coords,
  swapped axes, ``east < west``, ``south > north``, out-of-range).

Intersections are computed in EPSG:4326; areas and lengths are measured in
EPSG:3035 (a projection optimized to preserve area in Europe).

Algorithm
---------
Each bbox is intersected with **all** countries, not just the European set:
the non-European overlap is what distinguishes intentional European content
from an accidental border clip. Rationale and threshold choices live in
``docs/adr/0001-bbox-classifier-design-choices.md``.

STEP 0 — LOAD (once)
    EU set = ``(continent == "Europe" AND status == "Member State")
    ∪ {Turkey, Cyprus}``; Russia is clipped to west of 60°E.  
    Non-EU set = all other countries.

STEP 1 — PARSE
    ``"[west, south, east, north]"`` → coordinates.  ``None`` if unparseable.

STEP 1.0 — VALIDATION GATE
    Reject non-WGS-84 coordinates as ``invalid`` before any geometry work:
    ``abs(w)>180 | abs(e)>180 | abs(s)>90 | abs(n)>90 | east<west |
    south>north``.  Strict ``<`` so degenerate bboxes (``west==east`` etc.)
    pass through to Step 1.5.

STEP 1.5 — POINT / LINE SHORT-CIRCUIT
    A degenerate bbox — a point (``west==east`` AND ``south==north``) or a
    line (exactly one axis collapsed) — is resolved by containment (point)
    or length (line), not area.  Only EU countries are checked.
    See :func:`_classify_point_bbox`, :func:`_classify_line_bbox`.

STEP 2 — COORDINATE PRE-CHECK
    Keep only countries whose ``.bounds`` overlaps the bbox (pure arithmetic,
    no geometry ops).  If empty EU candidate set → classify as ``non_european``.

STEP 3 — GEOMETRIC INTERSECTION
    EU candidates: full intersection reprojected to EPSG:3035 (area overlap + coverage fraction).  
    Non-EU candidates: boolean check only (``area > 0`` in EPSG:4326, break on first hit).

    The non-EU result routes the bbox onto one of two paths:

    - **Path A** — only EU countries are hit → no competition, relevance test
      skipped, proceed to Step 5.
    - **Path B** — both EU *and* non-EU countries are hit → run Step 4.

STEP 4 — RELEVANCE TEST (Path B only)
    Decide whether the EU overlap is intentional: at least one EU country
    must have **coverage ≥ 50%** *and* those meaningful countries must hold
    **collective_share ≥ 10%** of the EU intersection area.  Fails →
    ``non_european``.  See :func:`_relevance_test`.

STEP 5 — SINGLE- vs MULTI-EUROPEAN
    Exactly 1 EU hit → ``single_country_eu``.  
    With 2+ EU hits, :func:`_classify_intersections` applies these rules in order:

    1. **Raw dominance** — if the largest intersection measure is more than
       *cutoff* times the runner-up, one country dominates → single.
       *Russia override*: Russia is so large it would dominate almost any
       pan-European bbox by area alone, so when it appears alongside many
       countries the result is forced to ``multi_country_eu``.
    2. **Coverage-dominance fallback** — when no country
       wins on raw area (i.e. raw domninance ratio < cutoff), 
       compare how *fully* each country sits inside the
       bbox (coverage fraction).  If one country is far more fully covered
       than the others, it is the country of interest → single — this
       catches small countries entirely inside the bbox.
    3. **Default** → ``multi_country_eu``.

"""

import ast
from collections import namedtuple
import geopandas as gpd
from shapely.geometry import LineString, Point, box

EQUAL_AREA_CRS = "EPSG:3035"
GEOJSON_PATH = "./datadump/world-administrative-boundaries.geojson"

# --- multi-country classification thresholds ---
_COVERAGE_RATIO_THRESHOLD = 3.0       # coverage-dominance fallback (Step 5)
_COVERAGE_RAW_SHARE_MIN = 0.10        #   ...and the top country's raw share minimum

# --- relevance-test thresholds (Step 4, Path B) ---
_MEANINGFUL_COVERAGE_THRESHOLD = 0.50  # is this country "substantially inside" the bbox
_COLLECTIVE_SHARE_THRESHOLD = 0.10     # meaningful countries' share of EU intersection

# --- country-set membership ---
_TURKEY_NAME = "Turkey"
_CYPRUS_NAME = "Cyprus"
# Added to the EU set by name (both are classified "Asia" in the GeoJSON but are European states).
_EXTRA_EU_NAMES = {_TURKEY_NAME, _CYPRUS_NAME}
_RUSSIA_NAME = "Russian Federation"
_EUROPEAN_RUSSIA_CLIP_LON = 60.0  # Longitude clip: approx. the Urals; east of this is Siberia (non-European)

# A recorded EU intersection: name, area in EPSG:3035, and coverage fraction (=intersection area/total country area).  
# Coverage feeds the relevance test; area feeds classification.
_EuHit = namedtuple("_EuHit", ["name", "area", "coverage"])


def load_countries(geojson_path=GEOJSON_PATH):
    """Load country geometries and split into European and non-European sets.

    EU set = ``(continent == "Europe" AND status == "Member State") ∪ {Turkey, Cyprus}``.  
    Turkey and Cyprus are ``continent == "Asia"`` in the source data, so they are included by name.  
    Russia's geometry is clipped to west of ``_EUROPEAN_RUSSIA_CLIP_LON`` (60°E).

    Args:
        geojson_path: Path to a GeoJSON with columns ``continent``,
            ``status``, ``name``, ``geometry``.

    Returns:
        Tuple ``(eu_gdf, non_eu_gdf)`` — GeoDataFrames[``name``, ``geometry``]
        in EPSG:4326.
    """
    gdf = gpd.read_file(filename=geojson_path)
    eu_mask = (
        ((gdf["continent"] == "Europe") & (gdf["status"] == "Member State"))
        | (gdf["name"].isin(_EXTRA_EU_NAMES))
    )
    eu_gdf = gdf.loc[eu_mask, ["name", "geometry"]].reset_index(drop=True)
    non_eu_gdf = gdf.loc[~eu_mask, ["name", "geometry"]].reset_index(drop=True)

    russia_mask = eu_gdf["name"] == _RUSSIA_NAME
    if russia_mask.any():
        clip_polygon = box(-180, -90, _EUROPEAN_RUSSIA_CLIP_LON, 90)
        russia_idx = eu_gdf.loc[russia_mask].index[0]
        eu_gdf.at[russia_idx, "geometry"] = eu_gdf.at[russia_idx, "geometry"].intersection(clip_polygon)

    return eu_gdf, non_eu_gdf


def _parse_bbox(bbox_str):
    """Parse a bbox string into a ``(west, south, east, north)`` tuple.

    Returns ``None`` on any malformed input.
    """
    if not isinstance(bbox_str, str) or not bbox_str.strip():
        return None
    try:
        coords = ast.literal_eval(bbox_str.strip())
        if len(coords) == 4:
            return tuple(coords)
    except (ValueError, SyntaxError):
        pass
    return None


def _is_valid_wgs84(west, south, east, north):
    """Return ``True`` if the coordinates are usable WGS 84 degrees.

    Returns ``False`` when any coordinate is out of range or the axes are
    swapped — the signatures of projected/metric coordinates, wrapped
    globals, or inverted latitude:

    - ``abs(west) > 180`` or ``abs(east) > 180``
    - ``abs(south) > 90`` or ``abs(north) > 90``
    - ``east < west`` or ``south > north``

    Comparisons are strict ``<``, so a degenerate bbox (``west == east`` or
    ``south == north``) is **not** rejected here — it flows on to Step 1.5
    (point/line short-circuit).  A degenerate bbox that is also dirty (e.g.
    a metric point) is caught by the magnitude check first.
    """
    if abs(west) > 180 or abs(east) > 180:
        return False
    if abs(south) > 90 or abs(north) > 90:
        return False
    if east < west or south > north:
        return False
    return True


def _empty_result(label):
    """Build the uniform 5-key result dict for a label with no country detail.

    Used by every short-circuit path (``invalid``, ``non_european``) so the
    return contract stays uniform across all classification outcomes.
    """
    return {
        "classification": label,
        "countries": {},
        "ratio": None,
        "coverage_ratio": None,
        "coverage_top_country": None,
    }


def _bounds_overlaps(country_bounds, west, south, east, north):
    """Return ``True`` if a country's ``.bounds`` overlaps the query bbox.

    Pure coordinate arithmetic (no geometry construction) — the Step 2
    pre-check that skips countries which cannot geometrically intersect the
    bbox before any expensive intersection runs. 
    """
    minx, miny, maxx, maxy = country_bounds
    return not (maxx < west or minx > east or maxy < south or miny > north)


def _compute_country_areas(european_gdf):
    """Return a dict mapping country name to total area in EPSG:3035."""
    projected = european_gdf.to_crs(EQUAL_AREA_CRS)
    return dict(zip(projected["name"], projected.geometry.area))


def _relevance_test(eu_hits):
    """Decide whether a bbox's EU overlap is intentional or accidental.

    Runs only on Path B (the bbox hits both EU and non-EU countries).  Both
    gates must pass:

    - **coverage ≥ 50%** — at least one EU country is substantially inside
      the bbox, not just a border clip.
    - **collective_share ≥ 10%** — those meaningful countries contribute real
      area, so the result is not swung by tiny islands dwarfed by a large
      partial clip.

    Args:
        eu_hits: list of :data:`_EuHit` (name, area, coverage).

    Returns:
        ``True`` if the overlap is intentional (European), ``False`` if
        accidental (should be ``non_european``).
    """
    total_eu_area = sum(hit.area for hit in eu_hits)
    if total_eu_area == 0:
        return False
    meaningful = [hit for hit in eu_hits if hit.coverage >= _MEANINGFUL_COVERAGE_THRESHOLD]
    if not meaningful:
        return False
    collective_share = sum(hit.area for hit in meaningful) / total_eu_area
    return collective_share >= _COLLECTIVE_SHARE_THRESHOLD


def _classify_point_bbox(east, north, eu_gdf):
    """Classify a point bbox (``west == east`` and ``south == north``).

    Containment is exclusive: a point sits in at most one country, so there
    is no EU/non-EU competition and the relevance test never runs.  EU
    countries are checked with :meth:`BaseGeometry.covers` (not
    ``contains``) so a point exactly on a national border is still claimed.

    - 0 EU hits → ``non_european`` (point at sea or in a non-EU country)
    - 1 EU hit → ``single_country_eu``
    - 2+ EU hits → ``multi_country_eu`` (in case the GeoJSON carries
      overlapping/disputed polygons)

    Non-EU countries are never consulted: a point outside every EU country
    is ``non_european``.
    """
    point = Point(east, north)
    hits = []
    for _, row in eu_gdf.iterrows():
        if not _bounds_overlaps(row.geometry.bounds, east, north, east, north):
            continue
        if row.geometry.covers(point):
            hits.append(row["name"])

    if not hits:
        return _empty_result("non_european")
    if len(hits) == 1:
        return {
            "classification": "single_country_eu",
            "countries": {hits[0]: 1.0},
            "ratio": None,
            "coverage_ratio": None,
            "coverage_top_country": None,
        }
    return {
        "classification": "multi_country_eu",
        "countries": {name: round(1.0 / len(hits), 4) for name in hits},
        "ratio": None,
        "coverage_ratio": None,
        "coverage_top_country": None,
    }


def _classify_line_bbox(west, south, east, north, eu_gdf, country_areas,
                        cutoff, russia_country_threshold):
    """Classify a line bbox (exactly one of ``west==east`` / ``south==north``).

    Length-based, mirroring the area pipeline: for each EU country the
    intersection length is measured in EPSG:3035 and used for dominance.
    EU-only. non-EU countries are never consulted and the relevance test
    never runs.

    - 0 EU hits → ``non_european``
    - 1 EU hit → ``single_country_eu``
    - 2+ EU hits → raw length dominance only (``> cutoff`` → single, Russia
      override, else multi); the coverage fallback is skipped because
      ``length / total_country_area`` is meaningless.

    Known limitation: a line that runs mostly through a non-EU country but
    clips an EU border resolves to that single EU country. See
    ``docs/adr/0001-bbox-classifier-design-choices.md``.
    """
    line = LineString([(west, south), (east, north)])
    intersections = []
    for _, row in eu_gdf.iterrows():
        if not _bounds_overlaps(row.geometry.bounds, west, south, east, north):
            continue
        try:
            inter = row.geometry.intersection(line)
        except Exception:
            continue
        if inter.is_empty:
            continue
        inter_proj = gpd.GeoSeries([inter], crs="EPSG:4326").to_crs(EQUAL_AREA_CRS).iloc[0]
        length = inter_proj.length
        if length > 0:
            intersections.append((row["name"], length))

    if not intersections:
        return _empty_result(label="non_european")
    if len(intersections) == 1:
        return {
            "classification": "single_country_eu",
            "countries": {intersections[0][0]: 1.0},
            "ratio": None,
            "coverage_ratio": None,
            "coverage_top_country": None,
        }
    return _classify_intersections(
        intersections, country_areas, cutoff, russia_country_threshold,
        use_coverage_fallback=False,
    )


def _classify_intersections(intersections, country_areas, cutoff,
                            russia_country_threshold, use_coverage_fallback=True):
    """Step 5 in the algoritm: Apply the single/multi classification pipeline.

    Rules are evaluated in order:

    1. **Raw dominance** — if ``largest / runner_up > cutoff`` the bbox is
       dominated by one country → ``single_country_eu``.

       *Russia override*: even when the raw ratio exceeds the cutoff, if
       Russia is present alongside more than *russia_country_threshold*
       countries, override to ``multi_country_eu`` — Russia's area would
       otherwise dominate almost any pan-European bbox.
    2. **Coverage-dominance fallback** (polygons only; skipped for lines via
       ``use_coverage_fallback=False``) — compute the *coverage fraction*
       (``intersection_measure / total_country_area``) for each country,
       then take the ratio of the highest coverage to the second highest.
       If this *coverage ratio* exceeds ``_COVERAGE_RATIO_THRESHOLD``
       **and** the top country's raw share of total intersection exceeds
       ``_COVERAGE_RAW_SHARE_MIN``, classify as ``single_country_eu``.
       This catches small countries that are fully inside the bbox but 
       don't dominate by raw area — e.g. a Portugal bbox covers 100 % of Portugal 
       but only 18 % of Spain; raw ratio is 1.05 (i.e. both take up an equal 
       amount of space in the bbox) but coverage ratio is 5.4 (Portugal is more represented).
    3. **Default** → ``multi_country_eu``.

    Args:
        intersections: List of ``(country_name, measure)`` tuples, where
            *measure* is intersection area (polygons) or length (lines).
        country_areas: Dict mapping country name to total area in EPSG:3035.
            Unused when ``use_coverage_fallback`` is False.
        cutoff: Raw dominance ratio threshold.
        russia_country_threshold: Country count threshold for Russia override.
        use_coverage_fallback: When False (lines), skip the coverage branch —
            ``length / total_country_area`` is meaningless.
    """
    intersections.sort(key=lambda x: x[1], reverse=True)
    total_area = sum(a for _, a in intersections)
    country_shares = {name: round(area / total_area, 4) for name, area in intersections}
    country_names = set(country_shares)

    raw_ratio = intersections[0][1] / intersections[1][1] 

    base_result = {
        "countries": country_shares,
        "ratio": round(number=raw_ratio, ndigits=4),
    }

    if raw_ratio > cutoff:
        is_russia_override = (
            _RUSSIA_NAME in country_names
            and len(intersections) > russia_country_threshold
        )
        return {
            **base_result,
            "classification": "multi_country_eu" if is_russia_override else "single_country_eu",
            "coverage_ratio": None,
            "coverage_top_country": None,
        }

    # Lines stop here: coverage = length / total_country_area is meaningless.
    if not use_coverage_fallback:
        return {
            **base_result,
            "classification": "multi_country_eu",
            "coverage_ratio": None,
            "coverage_top_country": None,
        }

    coverages = [
        (name, raw_area, raw_area / country_areas[name])
        for name, raw_area in intersections
    ]
    coverages.sort(key=lambda x: x[2], reverse=True)

    top_name, top_raw_area, top_coverage = coverages[0] #country with highest coverage
    top_raw_share = top_raw_area / total_area

    coverage_ratio = (
        top_coverage / coverages[1][2]
        if len(coverages) >= 2 and coverages[1][2] > 0
        else None
    )

    if coverage_ratio is not None and coverage_ratio > _COVERAGE_RATIO_THRESHOLD and top_raw_share > _COVERAGE_RAW_SHARE_MIN:
        return {
            **base_result,
            "classification": "single_country_eu",
            "coverage_ratio": round(number=coverage_ratio, ndigits=3),
            "coverage_top_country": top_name,
        }

    return {
        **base_result,
        "classification": "multi_country_eu",
        "coverage_ratio": round(number=coverage_ratio, ndigits=3) if coverage_ratio is not None else None,
        "coverage_top_country": top_name,
    }


def classify_bbox(bbox_str, eu_gdf, non_eu_gdf, country_areas=None,
                  cutoff=2.0, russia_country_threshold=10):
    """Classify a single bbox by its overlap with European countries.

    Implements the pipeline described in the module docstring (Steps 1–5):
    parse, validate, short-circuit degenerate cases, intersect, then hand
    multi-country cases to :func:`_classify_intersections`.

    Args:
        bbox_str: String like ``"[west, south, east, north]"``.
        eu_gdf: European countries GeoDataFrame from :func:`load_countries`.
        non_eu_gdf: Non-European countries GeoDataFrame from
            :func:`load_countries`.
        country_areas: Dict mapping EU country name to total area in
            EPSG:3035.  Computed from *eu_gdf* if not provided.
        cutoff: Raw dominance ratio threshold (default 2.0).
        russia_country_threshold: Pan-European override threshold
            (default 10 — bboxes with Russia + more than 10 countries
            are classified as multi-country regardless of ratio).

    Returns:
        ``None`` if unparseable, else ``dict`` with keys:

        - ``classification`` (str): one of ``"single_country_eu"``,
          ``"multi_country_eu"``, ``"non_european"``, ``"invalid"``.
        - ``countries`` (dict): mapping of country name to its share of
          total EU intersection area in the bbox (empty for ``invalid``/``non_european``).
        - ``ratio`` (float or None): raw dominance ratio (largest /
          second-largest intersection measure).
        - ``coverage_ratio`` (float or None): ratio of the top country's
          coverage fraction to the runner-up's.  ``None`` when the raw
          ratio settled it (i.e. raw ratio > cutoff) or for point/line bboxes.
        - ``coverage_top_country`` (str or None): country with the highest
          coverage fraction.
    """
    bbox = _parse_bbox(bbox_str)
    if bbox is None:
        return None
    west, south, east, north = bbox

    # Step 1.0 — validation gate: reject non-WGS-84 before any geometry work.
    if not _is_valid_wgs84(west, south, east, north):
        return _empty_result("invalid")

    if country_areas is None:
        country_areas = _compute_country_areas(european_gdf=eu_gdf)

    # Step 1.5 — point/line short-circuit
    lon_collapsed = west == east
    lat_collapsed = south == north
    if lon_collapsed and lat_collapsed:
        return _classify_point_bbox(east, north, eu_gdf)
    if lon_collapsed != lat_collapsed:  # XOR — exactly one axis collapsed
        return _classify_line_bbox(
            west, south, east, north, eu_gdf, country_areas,
            cutoff, russia_country_threshold,
        )

    # Step 2 — coordinate pre-check: keep only countries that can intersect.
    eu_candidates = [
        (row["name"], row.geometry)
        for _, row in eu_gdf.iterrows()
        if _bounds_overlaps(row.geometry.bounds, west, south, east, north)
    ]
    if not eu_candidates:
        return _empty_result(label="non_european")

    bbox_polygon = box(west, south, east, north)

    # Step 3 (EU) — full intersection: areas + coverage feed classification.
    eu_hits: list[_EuHit] = []
    for name, geom in eu_candidates:
        try:
            inter = bbox_polygon.intersection(geom)
        except Exception:
            continue
        if inter.is_empty:
            continue
        inter_projected = (
            gpd.GeoSeries([inter], crs="EPSG:4326")
            .to_crs(EQUAL_AREA_CRS)
            .iloc[0]
        )
        area = inter_projected.area
        if area > 0:
            coverage = area / country_areas[name]
            eu_hits.append(_EuHit(name, area, coverage))

    if not eu_hits:
        return _empty_result(label="non_european")

    # Step 3 (non-EU) — boolean only: areas never needed, break on first hit.
    # Intersection emptiness is projection-independent, so EPSG:4326 suffices.
    has_non_eu = False
    for _, row in non_eu_gdf.iterrows():
        if not _bounds_overlaps(row.geometry.bounds, west, south, east, north):
            continue
        try:
            inter = bbox_polygon.intersection(row.geometry)
        except Exception:
            continue
        if (not inter.is_empty) and inter.area > 0:
            has_non_eu = True
            break

    # Step 4 — relevance test (Path B): is the EU overlap intentional?
    if has_non_eu and not _relevance_test(eu_hits):
        return _empty_result(label="non_european")

    # Step 5 — single vs multi (EU subset only: non-EU never dilutes ratios).
    intersections = [(hit.name, hit.area) for hit in eu_hits]
    if len(intersections) == 1:
        return {
            "classification": "single_country_eu",
            "countries": {intersections[0][0]: 1.0},
            "ratio": None,
            "coverage_ratio": None,
            "coverage_top_country": None,
        }
    return _classify_intersections(
        intersections, country_areas, cutoff, russia_country_threshold
    )


def classify_bboxes(df, spatial_col="spatial", geojson_path=GEOJSON_PATH,
                    cutoff=2.0, russia_country_threshold=10):
    """Apply :func:`classify_bbox` to every row in a DataFrame column.

    Loads country geometries once, computes EU-only areas, then classifies
    each bbox in *spatial_col*.

    Args:
        df: DataFrame containing a column with bbox strings.
        spatial_col: Name of the column holding bbox strings.
        geojson_path: Path to the world boundaries GeoJSON.
        cutoff: Raw dominance ratio threshold (default 2.0).
        russia_country_threshold: Pan-European override threshold
            (default 10 countries).

    Returns:
        List of classification dicts (``None`` for unparseable rows).
    """
    eu_gdf, non_eu_gdf = load_countries(geojson_path)
    country_areas = _compute_country_areas(european_gdf=eu_gdf)
    return [
        classify_bbox(
            bbox_str=val,
            eu_gdf=eu_gdf,
            non_eu_gdf=non_eu_gdf,
            country_areas=country_areas,
            cutoff=cutoff,
            russia_country_threshold=russia_country_threshold,
        )
        for val in df[spatial_col]
    ]
