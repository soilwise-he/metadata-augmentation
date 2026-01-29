# Spatial Metadata Extraction Pipeline

## Overview

This pipeline processes CSV records containing spatial data URLs, validates their availability, and extracts spatial metadata using GDAL.

---
## Process Flow
### 1. Record-Level Filtering

- **Read CSV row by row**
- **Check for URLs**: Skip records with no valid URLs in the `links` field
- **Check existing geometry**: Skip records that already have `wkt_geometry` populated
- **Process each URL** in the remaining records

---

### 2. URL Validation Phase

**Validate URL** using `LinkLivelinessChecker`

- **Invalid** → Log error and write to output JSONL
- **Valid** → Extract `mediatype` and `gis_capabilities`

---

### 3. Type-Based Processing

#### Check for OGC Capabilities

**OGC service detected** (WMS/WFS/WCS)

- Extract capabilities metadata directly
- Skip GDAL processing

**Not OGC service**

- Check if mediatype is spatial (GeoJSON, Shapefile, GeoTIFF, etc.)
  - **Not spatial** → Skip GDAL processing
  - **Spatial** → Proceed to GDAL extraction

---

### 4. GDAL Extraction Phase

**Open data source** with GDAL

- **Success** → Extract vector or raster metadata
- **Failure** → Log GDAL error and write to output

---

### 5. Output Generation

All results written to JSONL format:

```json
{
  "identifier": "uuid",
  "url": "https://...",
  "metadata": {...},
  "date": "2025-12-19T...",
  "process": "spatial-extractor",
  "error": null
}
```

**Output includes:**

- Successfully extracted metadata
- GDAL errors
- Invalid URLs
- Processing exceptions

---

### 6. Summary Statistics

Final report includes:

- Total rows processed
- Successfully extracted metadata
- GDAL errors
- Valid/invalid URLs
- Skipped records

## Flowchart

## Usage

### Prerequisites
```bash
# Install required dependencies
pip install gdal aiohttp asyncio
```
## Flowchart

![Flowchart](Flowchart.png)

### Basic Usage

### Command-Line Arguments

| Argument          | Description                           | Default                            |
|-------------------|---------------------------------------|------------------------------------|
| `csv_file`        | Path to input CSV file (required)     |                                    |
| `--output`        | Output JSONL file path                | `spatial_metadata_augmented.jsonl` |
| `--start-from`    | Skipp already processed rows          | None (starts at 1) 
| `--limit`         | Maximum number of records to process  | None (process all)                 |
| `--no-link-check` | Skip URL validation step              | False (validation enabled)         |

### Example
```bash
# Process first 10 records with link validation
python spatial_metadata_processor.py datasets.csv --output results.jsonl --start-from 1 --limit 10
```