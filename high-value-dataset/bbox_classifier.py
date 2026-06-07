"""Classify WGS 84 bounding boxes by their overlap with European countries.

Each bbox is labelled as "single_country_eu", "multi_country_eu", or
"non_european".  Area calculations use EPSG:3035 (equal-area).
"""

import ast

import geopandas as gpd
from shapely.geometry import box

EQUAL_AREA_CRS = "EPSG:3035"
GEOJSON_PATH = "./datadump/world-administrative-boundaries.geojson"
EUROPEAN_BBOX = (-30, 30, 60, 75)


def load_european_countries(geojson_path=GEOJSON_PATH):
    """Load European member-state geometries, reprojected to EPSG:3035.

    Args:
        geojson_path: Path to a GeoJSON with columns ``continent``,
            ``status``, ``name``, ``geometry``.

    Returns:
        GeoDataFrame[``name``, ``geometry``] filtered to European
        member states, in EPSG:3035.
    """
    gdf = gpd.read_file(filename=geojson_path)
    return (
        gdf[
            (gdf["continent"] == "Europe")
            & (gdf["status"] == "Member State")
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

    Pre-filter to skip geometry work for non-European bboxes.
    """
    clon = (west + east) / 2
    clat = (south + north) / 2
    ew, es, ee, en = EUROPEAN_BBOX
    return ew <= clon <= ee and es <= clat <= en


def classify_bbox(bbox_str, european_gdf, cutoff=2.0):
    """Classify a single bbox by its overlap with European countries.

    1. Parse the bbox; return ``None`` if unparseable.
    2. Skip non-European bboxes via centroid check.
    3. Reproject to EPSG:3035 and intersect with each country.
    4. Label as ``"single_country_eu"`` when only one country overlaps or the
       dominance ratio (largest / second-largest area) exceeds *cutoff*,
       otherwise ``"multi_country_eu"``.

    Args:
        bbox_str: String like ``"[west, south, east, north]"``.
        european_gdf: GeoDataFrame from :func:`load_european_countries`.
        cutoff: Dominance ratio threshold (default 2.0 — largest country must
            cover ≥2× the runner-up to count as single-country).

    Returns:
        ``None`` if unparseable, else ``dict`` with keys:
        ``classification`` (str), ``countries`` (dict[name, share]),
        ``ratio`` (float or ``None``).
    """
    bbox = _parse_bbox(bbox_str)
    if bbox is None:
        return None

    west, south, east, north = bbox

    if not _centroid_in_europe(west, south, east, north):
        return {
            "classification": "non_european",
            "countries": {},
            "ratio": None
        }

    # Reproject bbox polygon to equal-area CRS for meaningful area values.
    bbox_polygon = box(west, south, east, north)
    bbox_projected = (
        gpd.GeoSeries([bbox_polygon], crs="EPSG:4326")
        .to_crs(EQUAL_AREA_CRS)
        .iloc[0]
    )

    # Intersect with each country, collecting non-empty (name, area) pairs.
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
        }

    # Sort descending by area; compute each country's share of total overlap.
    intersections.sort(key=lambda x: x[1], reverse=True)
    total_area = sum(a for _, a in intersections)
    countries = {name: round(area / total_area, 4) for name, area in intersections}

    if len(intersections) == 1:
        return {
            "classification": "single_country_eu",
            "countries": countries,
            "ratio": None,
        }

    # Dominance ratio: largest overlap vs runner-up.
    ratio = intersections[0][1] / intersections[1][1]
    classification = (
        "single_country_eu" if ratio > cutoff else "multi_country_eu"
    )

    return {
        "classification": classification,
        "countries": countries,
        "ratio": round(ratio, 3),
    }


def classify_bboxes(df, spatial_col="spatial", geojson_path=GEOJSON_PATH, cutoff=2.0):
    """Apply :func:`classify_bbox` to every row in a DataFrame column.

    Loads country geometries once, then classifies each bbox in *spatial_col*.

    Returns:
        List of classification dicts (``None`` for unparseable rows).
    """
    european_gdf = load_european_countries(geojson_path)
    return [classify_bbox(bbox_str=val, european_gdf=european_gdf, cutoff=cutoff) for val in df[spatial_col]]

