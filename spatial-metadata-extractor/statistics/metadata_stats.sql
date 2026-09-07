-- ============================================================================
-- Spatial Metadata Extraction — Aggregate Statistics
-- Run against the same Postgres DB used by spatial_metadata_pipeline.py
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1. Overall status breakdown (from metadata.augment_status)
-- ----------------------------------------------------------------------------
SELECT
    status,
    COUNT(*) AS record_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct
FROM metadata.augment_status
WHERE process = 'spatial-extractor'
GROUP BY status
ORDER BY record_count DESC;


-- ----------------------------------------------------------------------------
-- 2. How many augmented records actually have a bbox?
-- ----------------------------------------------------------------------------
SELECT
    COUNT(*) AS total_augmented,
    COUNT(*) FILTER (WHERE metadata ? 'bbox' AND metadata->'bbox' IS NOT NULL) AS with_bbox,
    COUNT(*) FILTER (WHERE NOT (metadata ? 'bbox') OR metadata->'bbox' = 'null'::jsonb) AS without_bbox,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE metadata ? 'bbox' AND metadata->'bbox' IS NOT NULL)
        / COUNT(*), 1
    ) AS pct_with_bbox
FROM metadata.augments
WHERE process = 'spatial-extractor';


-- ----------------------------------------------------------------------------
-- 3. Records that errored out — grouped by error message
--    (helps spot which servers/format issues are most common)
-- ----------------------------------------------------------------------------
SELECT
    metadata->>'error' AS error_message,
    COUNT(*) AS occurrences
FROM metadata.augments
WHERE process = 'spatial-extractor'
  AND metadata ? 'error'
GROUP BY metadata->>'error'
ORDER BY occurrences DESC
LIMIT 25;


-- ----------------------------------------------------------------------------
-- 4. Service type breakdown (wms / wfs / wcs / wmts / ogcapi / raster / vector)
-- ----------------------------------------------------------------------------
SELECT
    COALESCE(metadata->>'service_type', metadata->>'type', 'unknown') AS kind,
    COUNT(*) AS record_count
FROM metadata.augments
WHERE process = 'spatial-extractor'
GROUP BY kind
ORDER BY record_count DESC;


-- ----------------------------------------------------------------------------
-- 5. CRS coverage — how many records report 4326 / 3857 support
-- ----------------------------------------------------------------------------
SELECT
    COUNT(*) FILTER (WHERE metadata->>'crs4326' = 'true') AS has_crs4326,
    COUNT(*) FILTER (WHERE metadata->>'crs3857' = 'true') AS has_crs3857,
    COUNT(*) AS total_with_service_type
FROM metadata.augments
WHERE process = 'spatial-extractor'
  AND metadata ? 'service_type';


-- ----------------------------------------------------------------------------
-- 6. Records with NO layer matched (service-level fallback only)
--    i.e. layer_name is null but service_type exists — these are the
--    "connected but couldn't find the specific layer" cases
-- ----------------------------------------------------------------------------
SELECT
    COUNT(*) AS unmatched_layer_count
FROM metadata.augments
WHERE process = 'spatial-extractor'
  AND metadata ? 'service_type'
  AND (metadata->>'layer_name' IS NULL OR metadata->'layer_name' = 'null'::jsonb);


-- ----------------------------------------------------------------------------
-- 7. Top 20 record_ids with the most rows in augments
--    (helps spot which datasets have many duplicate distributions)
-- ----------------------------------------------------------------------------
SELECT
    record_id,
    COUNT(*) AS row_count
FROM metadata.augments
WHERE process = 'spatial-extractor'
GROUP BY record_id
ORDER BY row_count DESC
LIMIT 20;


-- ----------------------------------------------------------------------------
-- 8. Records processed per day (trend over time)
-- ----------------------------------------------------------------------------
SELECT
    date_trunc('day', date) AS day,
    COUNT(*) AS records_written
FROM metadata.augments
WHERE process = 'spatial-extractor'
GROUP BY day
ORDER BY day;


-- ----------------------------------------------------------------------------
-- 9. Vector vs raster breakdown (for non-OGC / GDAL-extracted records)
-- ----------------------------------------------------------------------------
SELECT
    metadata->>'type' AS gdal_type,
    metadata->>'driver' AS gdal_driver,
    COUNT(*) AS record_count
FROM metadata.augments
WHERE process = 'spatial-extractor'
  AND metadata ? 'type'
GROUP BY metadata->>'type', metadata->>'driver'
ORDER BY record_count DESC;


-- ----------------------------------------------------------------------------
-- 10. Full bbox extent across ALL augmented records (sanity check / map of coverage)
--     Only works cleanly if bbox arrays are [minx, miny, maxx, maxy, ...]
-- ----------------------------------------------------------------------------
SELECT
    MIN((metadata->'bbox'->>0)::float) AS overall_minx,
    MIN((metadata->'bbox'->>1)::float) AS overall_miny,
    MAX((metadata->'bbox'->>2)::float) AS overall_maxx,
    MAX((metadata->'bbox'->>3)::float) AS overall_maxy
FROM metadata.augments
WHERE process = 'spatial-extractor'
  AND metadata ? 'bbox'
  AND jsonb_typeof(metadata->'bbox') = 'array'
  AND jsonb_array_length(metadata->'bbox') >= 4;