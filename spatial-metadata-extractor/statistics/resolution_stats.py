"""
resolution_stats.py
--------------------
Prints a table summarising, for each source type (raster / vector / wms /
wmts / wfs / wcs), how the resolution heuristic works and how often it
actually produced a value in metadata.augments.

Usage:
    python resolution_stats.py
"""

import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    'host':     os.getenv('DB_HOST', 'localhost'),
    'port':     int(os.getenv('DB_PORT', 5432)),
    'dbname':   os.getenv('DB_NAME', 'postgres'),
    'user':     os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', ''),
}

# Static methodology description per type — paired with live counts below.
METHODOLOGY = [
    {
        'type':       'Raster',
        'heuristic':  'Pixel size',
        'how':        "Read from the file's own geotransform header via GDAL",
        'nature':     'Direct',
        'confidence': 'High',
    },
    {
        'type':       'Vector',
        'heuristic':  'Coordinate precision / feature density',
        'how':        'Precision: max decimal digits in a sample of feature coords. '
                       'Density: feature_count / bbox_area',
        'nature':     'Heuristic',
        'confidence': 'Low',
    },
    {
        'type':       'WMS',
        'heuristic':  'Scale hint',
        'how':        "GetCapabilities XML, ScaleHint / min-max scale denominator",
        'nature':     'Heuristic',
        'confidence': 'Medium',
    },
    {
        'type':       'WMTS',
        'heuristic':  'Scale denominators',
        'how':        "Capabilities XML, ScaleDenominator per TileMatrix zoom level",
        'nature':     'Heuristic',
        'confidence': 'Medium',
    },
    {
        'type':       'WFS',
        'heuristic':  'Precision hint',
        'how':        'DescribeFeatureType XML, text scan for "precision"/"decimal"',
        'nature':     'Heuristic',
        'confidence': 'Very Low',
    },
    {
        'type':       'WCS',
        'heuristic':  'Grid offset vectors',
        'how':        'DescribeCoverage XML, offsetVector values from grid definition',
        'nature':     'Direct',
        'confidence': 'High',
    },
]

# One aggregate query — one row back, 12 counts (total + has_resolution per type).
STATS_QUERY = """
    SELECT
        COUNT(*) FILTER (WHERE metadata->>'type' = 'raster')
            AS raster_total,
        COUNT(*) FILTER (
            WHERE metadata->>'type' = 'raster'
              AND metadata ? 'pixel_size'
              AND metadata->'pixel_size' <> 'null'::jsonb
        ) AS raster_has_value,

        COUNT(*) FILTER (WHERE metadata->>'type' = 'vector')
            AS vector_total,
        COUNT(*) FILTER (
            WHERE metadata->>'type' = 'vector'
              AND jsonb_typeof(metadata->'layers') = 'array'
              AND EXISTS (
                    SELECT 1 FROM jsonb_array_elements(metadata->'layers') AS layer
                    WHERE layer->>'coordinate_precision' IS NOT NULL
                      AND layer->>'coordinate_precision' <> 'null'
              )
        ) AS vector_has_value,

        COUNT(*) FILTER (WHERE metadata->>'service_type' = 'wms')
            AS wms_total,
        COUNT(*) FILTER (
            WHERE metadata->>'service_type' = 'wms'
              AND metadata ? 'scale_hint'
              AND metadata->'scale_hint' <> 'null'::jsonb
        ) AS wms_has_value,

        COUNT(*) FILTER (WHERE metadata->>'service_type' = 'wmts')
            AS wmts_total,
        COUNT(*) FILTER (
            WHERE metadata->>'service_type' = 'wmts'
              AND jsonb_typeof(metadata->'pixel_sizes') = 'array'
              AND jsonb_array_length(metadata->'pixel_sizes') > 0
        ) AS wmts_has_value,

        COUNT(*) FILTER (WHERE metadata->>'service_type' = 'wfs')
            AS wfs_total,
        COUNT(*) FILTER (
            WHERE metadata->>'service_type' = 'wfs'
              AND metadata ? 'coordinate_precision'
              AND metadata->'coordinate_precision' <> 'null'::jsonb
        ) AS wfs_has_value,

        COUNT(*) FILTER (WHERE metadata->>'service_type' = 'wcs')
            AS wcs_total,
        COUNT(*) FILTER (
            WHERE metadata->>'service_type' = 'wcs'
              AND jsonb_typeof(metadata->'grid_spacing') = 'array'
              AND jsonb_array_length(metadata->'grid_spacing') > 0
        ) AS wcs_has_value
    FROM metadata.augments
    WHERE process = 'spatial-extractor'
"""


def fetch_counts(conn):
    with conn.cursor() as cur:
        cur.execute(STATS_QUERY)
        cols = [d.name for d in cur.description]
        row = cur.fetchone()
    return dict(zip(cols, row))


def build_rows(counts):
    key_prefix = {
        'Raster': 'raster',
        'Vector': 'vector',
        'WMS':    'wms',
        'WMTS':   'wmts',
        'WFS':    'wfs',
        'WCS':    'wcs',
    }
    rows = []
    for entry in METHODOLOGY:
        prefix = key_prefix[entry['type']]
        total = counts[f'{prefix}_total']
        has_value = counts[f'{prefix}_has_value']
        pct = (100.0 * has_value / total) if total else 0.0
        rows.append({
            **entry,
            'records': total,
            'with_resolution': has_value,
            'coverage_pct': pct,
        })
    return rows


def print_table(rows):
    headers = ['#', 'Type', 'Heuristic', 'Nature', 'Confidence', 'Records', 'With Resolution', 'Coverage %']
    table = []
    for i, r in enumerate(rows, 1):
        table.append([
            str(i),
            r['type'],
            r['heuristic'],
            r['nature'],
            r['confidence'],
            str(r['records']),
            str(r['with_resolution']),
            f"{r['coverage_pct']:.1f}%",
        ])

    widths = [max(len(h), *(len(row[i]) for row in table)) for i, h in enumerate(headers)]

    def fmt_row(cells):
        return ' | '.join(c.ljust(w) for c, w in zip(cells, widths))

    sep = '-+-'.join('-' * w for w in widths)

    print(fmt_row(headers))
    print(sep)
    for row in table:
        print(fmt_row(row))

    print()
    print("How It Is Calculated:")
    for i, r in enumerate(rows, 1):
        print(f"  {i}. {r['type']}: {r['how']}")


def main():
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        counts = fetch_counts(conn)
    finally:
        conn.close()

    rows = build_rows(counts)
    print_table(rows)


if __name__ == "__main__":
    main()
