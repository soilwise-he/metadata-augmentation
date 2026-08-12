import os
import csv
import json
import asyncio
import psycopg2
import psycopg2.extras
from pathlib import Path
from datetime import datetime
import argparse
from link_liveliness_checker import AsyncURLChecker
from gdal_metadata import GDALMetadataExtractor
from adapter import get_adapter
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()  # Load env variables

csv.field_size_limit(int(1e8))

SPATIAL_MEDIATYPES = {
    'application/x-shapefile',
    'application/geopackage+sqlite3',
    'application/vnd.google-earth.kml+xml',
    'application/geo+json',
    'application/gml+xml',
    'image/tiff',
    'image/geotiff',
    'application/x-netcdf',
    'application/netcdf',
    'application/zip',
    'application/x-zip-compressed',
}

# Database connection and operations
def get_db_connection(db_config: dict):
    """Create PostgreSQL connection"""
    return psycopg2.connect(**db_config)

def insert_augment_record(conn, record_id: str, property_name: str, value: str, process: str = 'spatial-extractor'):
    """Insert into metadata.augments table"""
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO metadata.augments (record_id, property, value, process, date) VALUES (%s, %s, %s, %s, now())",
            (record_id, property_name, value, process)
        )
    conn.commit()

def insert_augment_status(conn, record_id: str, status: str, process: str = 'spatial-extractor'):
    with conn.cursor() as cur:
        cur.execute(
            """INSERT INTO metadata.augment_status (record_id, status, process, date)
               VALUES (%s, %s, %s, now())
               ON CONFLICT (record_id, process) DO UPDATE SET status = EXCLUDED.status, date = now()""",
            (record_id, status, process)
        )
    conn.commit()

def write_metadata_to_db(conn, record_id: str, metadata: dict, status: str = 'success'):
    try:
        if metadata:
            with conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO metadata.augments (record_id, metadata, process, date)
                       VALUES (%s, %s, 'spatial-extractor', now())
                       ON CONFLICT (record_id, process) DO UPDATE 
                       SET metadata = EXCLUDED.metadata, date = now()""",
                    (record_id, json.dumps(metadata))
                )
        insert_augment_status(conn, record_id, status)
        conn.commit()
        return True
    except Exception as e:
        conn.rollback()
        print(f"Error writing to database: {e}")
        try:
            insert_augment_status(conn, record_id, f"error: {str(e)}")
        except Exception:
            conn.rollback()
        return False
    
async def check_url_validity(url: str, identifier: str = None, lname: str = None) -> dict:
    """Check if URL is valid using link checker"""
    async with AsyncURLChecker(timeout=10) as checker:
        return await checker.check_url(url, check_ogc_capabilities=True, identifier=identifier, lname=lname)
        
def ensure_record_exists(conn, record_id: str):
    """Insert record into metadata.records if it doesn't exist yet"""
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO metadata.records (identifier) VALUES (%s) ON CONFLICT (identifier) DO NOTHING",
            (record_id,)
        )
    conn.commit()

def process_records(db_config: dict, adapter, output_file=None, limit=None, check_links=True):
    """
    Process records from any adapter (PostgreSQL, CSV, Zenodo).
    
    1. Iterates records from the given adapter
    2. Validates URLs with link checker (unless skipped)
    3. Extracts spatial metadata with GDAL
    4. Writes results to metadata.augments and metadata.augment_status
    """
    extractor = GDALMetadataExtractor()
    stats = {'processed': 0, 'valid_urls': 0, 'invalid_urls': 0, 'skipped': 0, 'errors': 0}
    results = []

    db_conn = get_db_connection(db_config)
    output_f = open(output_file, 'w') if output_file else None
    rows_processed = 0

    try:
        for source_record in adapter:
            rows_processed += 1
            if limit and rows_processed > limit:
                print(f"\nReached limit of {limit} records, stopping...")
                break

            identifier = source_record.identifier
            url = source_record.url
            mediatype = source_record.mediatype
            
            # Skip .md and other non-spatial files before touching the DB
            if url.lower().endswith('.md'):
                print(f"[{rows_processed}] SKIP {identifier} — markdown file")
                continue

            ensure_record_exists(db_conn, identifier)

            # Skip if already successfully processed
            with db_conn.cursor() as cur:
                cur.execute(
                    """SELECT status FROM metadata.augment_status 
                    WHERE record_id = %s AND process = 'spatial-extractor'""",
                    (identifier,)
                )
                row = cur.fetchone()
                if row and row[0] in ('success', 'success_ogc'):
                    print(f"[{rows_processed}] ALREADY PROCESSED {identifier} (status: {row[0]}), skipping...")
                    stats['skipped'] += 1
                    continue
    
            print(f"\n[{rows_processed}] PROCESS {identifier}")
            print(f"URL: {url[:80]}...")

            try:
                link_result = asyncio.run(check_url_validity(url, identifier=identifier, lname=source_record.lname))
                
                if not link_result.get('valid') and check_links:
                    print(f"INVALID - Status: {link_result.get('status_code')}")
                    stats['invalid_urls'] += 1
                    write_metadata_to_db(db_conn, identifier, {'error': link_result.get('error')}, status='invalid')
                    continue

                mediatype = link_result.get('content_type') or mediatype
                stats['valid_urls'] += 1
                gis_capabilities = link_result.get('gis_capabilities')

                if gis_capabilities:
                    OGC_KEEP = {
                        'service_type', 'layer_name',
                        'title', 'abstract', 'keywords',
                        'bbox', 'crs4326', 'crs3857',
                        'metadata_urls', 'formats', 'schema'
                    }
                    db_metadata = {k: v for k, v in gis_capabilities.items() if k in OGC_KEEP}
                    db_metadata.update(source_record.extra)
                    write_metadata_to_db(db_conn, identifier, db_metadata, status='success_ogc')
                    stats['processed'] += 1
                    print(f"SUCCESS (OGC) - Layer: {db_metadata.get('layer_name')}")
                    continue  # ← skip GDAL entirely

            except Exception as e:
                print(f"OGC/link check error: {e}")
            try:
                result = extractor.process_url(url, mediatype)

                if result['success']:
                    metadata = result['metadata']
                    metadata.update(source_record.extra)
                    print(f"SUCCESS - Type: {metadata.get('type')}, Driver: {metadata.get('driver')}")
                    if metadata.get('type') == 'vector':
                        print(f"Layers: {metadata.get('layer_count')}")

                    # Filter to only meaningful fields
                    AUGMENT_KEEP = {
                        'type', 'driver', 'bbox', 'projection', 'epsg_code',
                        'pixel_size', 'width', 'height', 'band_count',
                        'layer_count', 'geometry_type',
                        'title', 'doi', 'zenodo_id', 'filename', 'filesize',
                    }
                    db_metadata = {k: v for k, v in metadata.items() if k in AUGMENT_KEEP}
                    write_metadata_to_db(db_conn, identifier, db_metadata, status='success')
                    results.append({'identifier': identifier, 'url': url, 'metadata': metadata})  # full version in output file
                    stats['processed'] += 1
                else:
                    error_msg = result.get('error', 'Unknown error')
                    if error_msg.startswith('Skipped:'):
                        print(f"SKIPPED: {error_msg}")
                        stats['skipped'] += 1
                    else:
                        error_msg = result.get('error', 'Unknown error')
                        print(f"GDAL ERROR: {error_msg[:80]}")
                        write_metadata_to_db(db_conn, identifier, {'error': error_msg}, status='gdal_error')
                        stats['errors'] += 1

                if output_f:
                    output_record = {
                        'identifier': identifier,
                        'url': url,
                        'metadata': result.get('metadata'),
                        'date': datetime.now(timezone.utc).isoformat(),
                        'process': 'spatial-extractor',
                        'error': result.get('error') if not result['success'] else None
                    }
                    output_f.write(json.dumps(output_record, default=str) + '\n')
                    output_f.flush()

            except Exception as e:
                print(f"EXCEPTION: {e}")
                write_metadata_to_db(db_conn, identifier, {'error': str(e)}, status='exception')
                stats['errors'] += 1

    finally:
        if output_f:
            output_f.close()
        db_conn.close()

    print(f"\n{'='*70}")
    print(f"SUMMARY:")
    print(f"Total records:          {rows_processed}")
    print(f"Successfully processed: {stats['processed']}")
    print(f"GDAL errors:            {stats['errors']}")
    print(f"Valid URLs:             {stats['valid_urls']}")
    print(f"Invalid URLs:           {stats['invalid_urls']}")
    print(f"Skipped:                {stats['skipped']}")
    print(f"{'='*70}")

    return results

# Database connection — defined internally
DB_CONFIG = {
    'host':     os.getenv('DB_HOST', 'localhost'),
    'port':     int(os.getenv('DB_PORT', 5432)),
    'dbname':   os.getenv('DB_NAME', 'postgres'),
    'user':     os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', ''),
}

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process spatial metadata from any source')

    # Only decide the source
    parser.add_argument('--source', choices=['postgresql', 'csv', 'zenodo'], default='postgresql', help='Source adapter to use')

    # CSV options
    parser.add_argument('--csv-file', help='Path to CSV file (required when --source=csv)')

    # Zenodo options
    parser.add_argument('--zenodo-ids', nargs='+', help='Specific Zenodo record IDs')
    parser.add_argument('--zenodo-query',     help='Zenodo search query (required when --source=zenodo)')
    parser.add_argument('--zenodo-community', help='Zenodo community slug (optional)')
    parser.add_argument('--zenodo-token',     help='Zenodo access token (optional)')
    parser.add_argument('--zenodo-max',       type=int, default=0, help='Max deposits to fetch (0 = no limit)')

    # General options
    parser.add_argument('--output',        help='Output JSONL file (optional)')
    parser.add_argument('--limit',         type=int, help='Limit records to process')
    parser.add_argument('--no-link-check', action='store_true', help='Skip link validation')

    args = parser.parse_args()

    # Test DB connection
    try:
        conn = get_db_connection(DB_CONFIG)
        conn.close()
        print("Database connection successful")
    except Exception as e:
        print(f"Database connection failed: {e}")
        exit(1)

    # Build adapter based on --source
    if args.source == 'postgresql':
        adapter = get_adapter('postgresql', db_config=DB_CONFIG)

    elif args.source == 'csv':
        if not args.csv_file:
            print("--csv-file is required when --source=csv")
            exit(1)
        adapter = get_adapter('csv', filepath=args.csv_file)

    elif args.source == 'zenodo':
        if not args.zenodo_ids and not args.zenodo_query:
            print("--zenodo-ids or --zenodo-query is required when --source=zenodo")
            exit(1)
        adapter = get_adapter(
            'zenodo',
            record_ids=args.zenodo_ids,
            search_query=args.zenodo_query,
            community=args.zenodo_community,
            access_token=args.zenodo_token,
            max_records=args.zenodo_max,
        )

    # Run
    process_records(DB_CONFIG, adapter, args.output, args.limit, check_links=not args.no_link_check)