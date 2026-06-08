"""Classify WGS 84 bounding boxes by their overlap with European countries.

Each bbox is labelled as "single_country_eu", "multi_country_eu", or
"non_european".  Area calculations use EPSG:3035 (equal-area).

Classification uses two complementary dominance signals:

**Raw dominance ratio** (ratio of largest / second-largest intersection area).
This catches cases where one country dominates the bbox by sheer area — e.g. a bbox
covering Germany plus small parts of its neighbours.  Large countries tend
to have high raw ratios even when the bbox also covers parts of others.

**Area-normalised dominance ratio** (calculate intersection_area / total_country_area,
then ratio of the top coverage fraction vs the runner-up). This catches cases where a
small country is entirely inside the bbox but doesn't dominate by raw
area because a larger neighbour contributes more absolute surface.  For
example, a Portugal bbox covers 100 % of Portugal but only 18 % of Spain;
the raw ratio is only 1.05 (Spain covers more area in the bbox) 
but the coverage ratio is 5.4 (Portugal is far more completely covered).

The two rules fire in sequence: raw ratio first (catches most cases),
then coverage ratio as a fallback for the ambiguous cases that the raw ratio
cannot resolve.
"""

import ast

import geopandas as gpd
from shapely.geometry import box

EQUAL_AREA_CRS = "EPSG:3035"
GEOJSON_PATH = "./datadump/world-administrative-boundaries.geojson"
EUROPEAN_BBOX = (-30, 30, 60, 75)

_TURKEY_OVERRIDE = "Turkey" #added to the list of European countries
_RUSSIA_NAME = "Russian Federation" #exceptions are made for Russia as it dominates most bboxes (wrongfully classifying the bbox as single-country)


def load_european_countries(geojson_path=GEOJSON_PATH):
    """Load European country geometries, reprojected to EPSG:3035.

    Includes European member states from the GeoJSON plus Turkey (an EU
    candidate country classified as ``continent == "Asia"`` in the source
    data).

    Args:
        geojson_path: Path to a GeoJSON with columns ``continent``,
            ``status``, ``name``, ``geometry``.

    Returns:
        GeoDataFrame[``name``, ``geometry``] in EPSG:3035.
    """
    gdf = gpd.read_file(filename=geojson_path)
    return (
        gdf[
            ((gdf["continent"] == "Europe") & (gdf["status"] == "Member State"))
            | (gdf["name"] == _TURKEY_OVERRIDE)
        ][["name", "geometry"]]
        .to_crs(EQUAL_AREA_CRS)
        .reset_index(drop=True)
    )


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


def _centroid_in_europe(west, south, east, north):
    """Return ``True`` if the bbox centroid falls within :data:`EUROPEAN_BBOX`.

    Pre-filter to skip geometry computation for non-European bboxes.
    """
    clon = (west + east) / 2
    clat = (south + north) / 2
    ew, es, ee, en = EUROPEAN_BBOX
    return ew <= clon <= ee and es <= clat <= en


def _compute_country_areas(european_gdf):
    """Return a dict mapping country name to total area in EPSG:3035."""
    return {row["name"]: row.geometry.area for _, row in european_gdf.iterrows()}


def _classify_intersections(intersections, country_areas, cutoff,
                            russia_country_threshold):
    """Apply the multi-rule classification pipeline.

    Rules are evaluated in order; the first match wins:

    1. **Raw dominance**: if ``largest_area / runner_up_area > cutoff``
       the bbox represents one (european) country.  An exception is
       made when Russia is present alongside many countries (see rule 2).
    2. **Russia override**: if Russia is among the
       intersecting countries and more than *russia_country_threshold*
       countries are present, override to ``multi_country_eu``.  This
       prevents Russia's massive area from inflating the raw ratio for
       pan-European bboxes that span 11–43 countries.
    3. **Coverage-dominance fallback**: compute *coverage fraction*
       (``intersection_area / total_country_area``) for each country, then
       take the ratio of the highest coverage to the second highest.  If this
       *coverage ratio* exceeds 3.0 **and** the top country's raw share
       of total intersection area exceeds 0.10, classify as
       ``single_country_eu``.  The latter catches small-country spillover where
       the country of interest is fully inside the bbox but doesn't
       dominate by raw area.
    4. **Default**: ``multi_country_eu``.
    """
    intersections.sort(key=lambda x: x[1], reverse=True)
    total_area = sum(a for _, a in intersections)
    countries = {name: round(area / total_area, 4) for name, area in intersections}
    country_names = set(countries)

    raw_ratio = intersections[0][1] / intersections[1][1]

    result_base = {
        "countries": countries,
        "ratio": round(number=raw_ratio, ndigits=4),
    }

    if raw_ratio > cutoff:
        if (
            _RUSSIA_NAME in country_names
            and len(intersections) > russia_country_threshold
        ):
            return {
                **result_base,
                "classification": "multi_country_eu",
                "coverage_ratio": None,
                "coverage_top_country": None,
            }
        else: 
            return {
                **result_base,
                "classification": "single_country_eu",
                "coverage_ratio": None,
                "coverage_top_country": None,
            }

    coverages = [
        (name, raw_area, raw_area / country_areas[name])
        for name, raw_area in intersections
    ]
    coverages.sort(key=lambda x: x[2], reverse=True)

    top_name, top_raw_area, top_coverage = coverages[0]
    top_raw_share = top_raw_area / total_area

    coverage_ratio = (
        top_coverage / coverages[1][2]
        if len(coverages) >= 2 and coverages[1][2] > 0
        else None
    )

    if coverage_ratio is not None and coverage_ratio > 3.0 and top_raw_share > 0.10:
        return {
            **result_base,
            "classification": "single_country_eu",
            "coverage_ratio": round(number=coverage_ratio, ndigits=3),
            "coverage_top_country": top_name,
        }

    return {
        **result_base,
        "classification": "multi_country_eu",
        "coverage_ratio": round(number=coverage_ratio, ndigits=3) if coverage_ratio is not None else None,
        "coverage_top_country": top_name,
    }


def classify_bbox(bbox_str, european_gdf, country_areas=None, cutoff=2.0,
                  russia_country_threshold=10):
    """Classify a single bbox by its overlap with European countries.

    This is the outer pipeline — parse, filter, intersect — then hand
    off multi-country cases to :func:`_classify_intersections`, which
    owns the dominance-ratio rules and edge-case overrides.

    Steps:

    1. Parse the bbox; return ``None`` if unparseable.
    2. Skip non-European bboxes via centroid check.
    3. Reproject to EPSG:3035 and intersect with each country.
    4. Single-country overlap → ``"single_country_eu"``.
    5. Two or more countries → delegate to
       :func:`_classify_intersections`.

    See :func:`_classify_intersections` for the multi-country rule
    details (raw dominance, Russia override, coverage-dominance fallback).

    Args:
        bbox_str: String like ``"[west, south, east, north]"``.
        european_gdf: GeoDataFrame from :func:`load_european_countries`.
        country_areas: Dict mapping country name to total area in
            EPSG:3035.  Computed from *european_gdf* if not provided.
        cutoff: Raw dominance ratio threshold (default 2.0).
        russia_country_threshold: Pan-European override threshold
            (default 10 — bboxes with Russia + more than 10 countries
            are classified as multi-country regardless of ratio).

    Returns:
        ``None`` if unparseable, else ``dict`` with keys:

        - ``classification`` (str): one of ``"single_country_eu"``,
          ``"multi_country_eu"``, ``"non_european"``.
        - ``countries`` (dict): mapping of country name to its share of
          total intersection area.
        - ``ratio`` (float or None): raw dominance ratio (largest area /
          second-largest area).
        - ``coverage_ratio`` (float or None): ratio of the top country's
          coverage fraction to the runner-up's.  ``None`` when the raw
          ratio already settled the classification.
        - ``coverage_top_country`` (str or None): country with the highest
          coverage fraction.  ``None`` when the raw ratio already settled
          the classification.
    """
    bbox = _parse_bbox(bbox_str)
    if bbox is None:
        return None

    west, south, east, north = bbox

    if not _centroid_in_europe(west, south, east, north):
        return {
            "classification": "non_european",
            "countries": {},
            "ratio": None,
            "coverage_ratio": None,
            "coverage_top_country": None,
        }

    bbox_polygon = box(west, south, east, north)
    bbox_projected = (
        gpd.GeoSeries([bbox_polygon], crs="EPSG:4326")
        .to_crs(EQUAL_AREA_CRS)
        .iloc[0]
    )

    intersections = []
    for _, row in european_gdf.iterrows():
        try:
            geom = bbox_projected.intersection(row.geometry)
        except Exception:
            continue
        if geom.is_empty:
            continue
        area = geom.area
        if area > 0:
            intersections.append((row["name"], area))

    if not intersections:
        return {
            "classification": "non_european",
            "countries": {},
            "ratio": None,
            "coverage_ratio": None,
            "coverage_top_country": None,
        }

    if len(intersections) == 1:
        total_area = intersections[0][1]
        return {
            "classification": "single_country_eu",
            "countries": {intersections[0][0]: 1.0},
            "ratio": None,
            "coverage_ratio": None,
            "coverage_top_country": None,
        }

    if country_areas is None:
        country_areas = _compute_country_areas(european_gdf)

    return _classify_intersections(
        intersections, country_areas, cutoff, russia_country_threshold
    )


def classify_bboxes(df, spatial_col="spatial", geojson_path=GEOJSON_PATH,
                    cutoff=2.0, russia_country_threshold=10):
    """Apply :func:`classify_bbox` to every row in a DataFrame column.

    Loads country geometries once and precomputes country areas, then
    classifies each bbox in *spatial_col*.

    Args:
        df: DataFrame containing a column with bbox strings.
        spatial_col: Name of the column holding bbox strings.
        geojson_path: Path to the world boundaries GeoJSON.
        cutoff: Raw dominance ratio threshold (default 2.0).
        russia_country_threshold: Pan-European override threshold
            (default 10).

    Returns:
        List of classification dicts (``None`` for unparseable rows).
    """
    european_gdf = load_european_countries(geojson_path)
    country_areas = _compute_country_areas(european_gdf)
    return [
        classify_bbox(
            bbox_str=val,
            european_gdf=european_gdf,
            country_areas=country_areas,
            cutoff=cutoff,
            russia_country_threshold=russia_country_threshold,
        )
        for val in df[spatial_col]
    ]
