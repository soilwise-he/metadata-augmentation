import os
import sys
import csv
import json
import time
import signal
import asyncio
import threading
import faulthandler
import psycopg2
import psycopg2.extras
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import argparse
from link_liveliness_checker import AsyncURLChecker
from gdal_metadata import GDALMetadataExtractor
from adapter import get_adapter, read_zenodo_ids_from_csv
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()  # Load env variables

# Some source records (identifiers/URLs/titles) contain characters the
# Windows console's active codepage can't display. Without this, printing
# one of those raises an unhandled UnicodeEncodeError inside a worker thread
# — the record silently fails instead of erroring visibly, which can look
# exactly like the pipeline got stuck.
for _stream in (sys.stdout, sys.stderr):
    if hasattr(_stream, 'reconfigure'):
        _stream.reconfigure(encoding='utf-8', errors='replace')

# If the pipeline looks stuck, press Ctrl+Break (Windows) — this dumps every
# thread's current stack trace to stderr without killing the process, so we
# can see exactly which line each worker is blocked on instead of guessing.
# faulthandler.register() needs sigaction (Unix-only), so wire the signal up
# manually and call dump_traceback() ourselves — that part works on Windows.
if hasattr(signal, 'SIGBREAK'):
    def _dump_all_thread_stacks(signum, frame):
        faulthandler.dump_traceback(all_threads=True)
    signal.signal(signal.SIGBREAK, _dump_all_thread_stacks)

# Simpler alternative to Ctrl+Break: while the pipeline is running, create an
# empty file named dump_stacks.flag next to this script. Within a few seconds
# every thread's current stack trace is printed and the flag file is removed.
_DUMP_STACKS_FLAG = Path(__file__).with_name('dump_stacks.flag')

def _watch_for_dump_flag():
    while True:
        time.sleep(3)
        try:
            if _DUMP_STACKS_FLAG.exists():
                faulthandler.dump_traceback(all_threads=True)
                _DUMP_STACKS_FLAG.unlink(missing_ok=True)
        except Exception:
            pass

csv.field_size_limit(int(1e8))

# Number of records processed concurrently in process_records(). Raise/lower
# to trade off throughput against load on the DB and remote servers/APIs.
MAX_WORKERS = 7

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
    """
    Create PostgreSQL connection.
    Keepalives let a silently-dropped connection (e.g. a firewall/NAT closing
    a long-idle socket) surface as an error instead of hanging forever on the
    next query; statement_timeout bounds any single query the same way.
    """
    return psycopg2.connect(
        connect_timeout=10,
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=10,
        keepalives_count=3,
        options='-c statement_timeout=30000',
        **db_config,
    )

# Thread-local DB connections — psycopg2 connections aren't thread-safe, so
# each worker thread gets its own, reused across the records it processes.
_thread_local = threading.local()
_thread_connections = []
_thread_connections_lock = threading.Lock()

def get_thread_db_connection(db_config: dict):
    """Return (creating if needed) the DB connection owned by the current thread."""
    conn = getattr(_thread_local, 'conn', None)
    if conn is None or conn.closed:
        conn = get_db_connection(db_config)
        _thread_local.conn = conn
        with _thread_connections_lock:
            _thread_connections.append(conn)
    return conn

def close_thread_connections():
    """Close every per-thread connection opened by get_thread_db_connection()."""
    with _thread_connections_lock:
        for conn in _thread_connections:
            try:
                if not conn.closed:
                    conn.close()
            except Exception:
                pass
        _thread_connections.clear()

def insert_augment_record(conn, record_id: str, property_name: str, value: str, process: str = 'spatial-extractor'):
    """Insert into metadata.augments table"""
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO metadata.augments (record_id, property, value, process, date) VALUES (%s, %s, %s, %s, now())",
            (record_id, property_name, value, process)
        )
    conn.commit()

def insert_augment_status(conn, record_id: str, url: str, status: str, process: str = 'spatial-extractor'):
    with conn.cursor() as cur:
        cur.execute(
            """INSERT INTO metadata.augment_status (record_id, url, status, process, date)
               VALUES (%s, %s, %s, %s, now())
               ON CONFLICT (record_id, url, process) DO UPDATE SET status = EXCLUDED.status, date = now()""",
            (record_id, url, status, process)
        )
    conn.commit()

def write_metadata_to_db(conn, record_id: str, url: str, metadata: dict, status: str = 'success'):
    try:
        if metadata:
            with conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO metadata.augments (record_id, url, metadata, process, date)
                       VALUES (%s, %s, %s, 'spatial-extractor', now())
                       ON CONFLICT (record_id, url, process) DO UPDATE
                       SET metadata = EXCLUDED.metadata, date = now()""",
                    (record_id, url, json.dumps(metadata))
                )
        insert_augment_status(conn, record_id, url, status)
        conn.commit()
        return True
    except Exception as e:
        conn.rollback()
        print(f"Error writing to database: {e}")
        try:
            insert_augment_status(conn, record_id, url, f"error: {str(e)}")
        except Exception:
            conn.rollback()
        return False
    
async def check_url_validity(url: str, identifier: str = None, lname: str = None) -> dict:
    """Check if URL is valid using link checker"""
    async with AsyncURLChecker(timeout=10) as checker:
        return await checker.check_url(url, check_ogc_capabilities=True, identifier=identifier, lname=lname)
        
def ensure_record_exists(conn, record_id: str) -> bool:
    """Insert record into metadata.records if it doesn't exist yet. Returns False (and
    rolls back) if the insert fails, e.g. record_id exceeds the identifier column's
    length limit — one bad identifier shouldn't abort the whole run."""
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO metadata.records (identifier) VALUES (%s) ON CONFLICT (identifier) DO NOTHING",
                (record_id,)
            )
        conn.commit()
        return True
    except Exception as e:
        conn.rollback()
        print(f"Error ensuring record exists for '{record_id}': {e}")
        return False

def _skip_status(reason: str) -> str:
    """Turn a human-readable skip/error reason into a short, queryable status
    value, so the specific cause is visible directly in augment_status.status
    instead of only inside the augments.metadata JSON."""
    reason_lower = reason.lower()
    if 'markdown' in reason_lower:
        return 'skipped_markdown'
    if 'mediatype' in reason_lower:
        return 'skipped_mediatype'
    if 'zip' in reason_lower:
        return 'skipped_zip'
    if 'extension' in reason_lower:
        return 'skipped_extension'
    return 'skipped'


def process_one_record(db_config: dict, extractor, source_record, row_num: int,
                        check_links: bool, output_f, output_lock) -> tuple:
    """
    Process a single record (runs on a worker thread). Mirrors the body of
    the old sequential process_records() loop, uses this thread's own DB
    connection, and guards output-file writes with output_lock.

    Returns (stats_delta, result_record_or_None).
    """
    stats_delta = {'processed': 0, 'valid_urls': 0, 'invalid_urls': 0, 'skipped': 0, 'errors': 0}

    identifier = source_record.identifier
    url = source_record.url
    mediatype = source_record.mediatype

    db_conn = get_thread_db_connection(db_config)

    if not ensure_record_exists(db_conn, identifier):
        print(f"[{row_num}] SKIP {identifier} — could not be stored (see error above)")
        stats_delta['errors'] += 1
        return stats_delta, None

    # Skip .md and other non-spatial files before hitting the network — but
    # still record why, so re-runs don't keep retrying the same dead end.
    if url.lower().endswith('.md'):
        reason = 'markdown file'
        print(f"[{row_num}] SKIP {identifier} — {reason}")
        stats_delta['skipped'] += 1
        write_metadata_to_db(db_conn, identifier, url, {'reason': reason}, status=_skip_status(reason))
        return stats_delta, None

    # Skip if this specific distribution (record_id + url) was already successfully processed
    with db_conn.cursor() as cur:
        cur.execute(
            """SELECT status FROM metadata.augment_status
            WHERE record_id = %s AND url = %s AND process = 'spatial-extractor'""",
            (identifier, url)
        )
        row = cur.fetchone()
        if row and row[0] in ('success', 'success_ogc'):
            print(f"[{row_num}] ALREADY PROCESSED {identifier} [{mediatype}] ({url}) (status: {row[0]}), skipping...")
            stats_delta['skipped'] += 1
            return stats_delta, None

    print(f"\n[{row_num}] PROCESS {identifier}")
    print(f"URL: {url}")

    try:
        link_result = asyncio.run(check_url_validity(url, identifier=identifier, lname=source_record.lname))

        if not link_result.get('valid') and check_links:
            print(f"INVALID - Status: {link_result.get('status_code')}")
            stats_delta['invalid_urls'] += 1
            write_metadata_to_db(db_conn, identifier, url, {'error': link_result.get('error')}, status='invalid')
            return stats_delta, None

        mediatype = link_result.get('content_type') or mediatype
        stats_delta['valid_urls'] += 1
        gis_capabilities = link_result.get('gis_capabilities')

        if gis_capabilities:
            OGC_KEEP = {
                'service_type', 'layer_name',
                'title', 'abstract', 'keywords',
                'bbox', 'crs4326', 'crs3857',
                'metadata_urls', 'formats', 'schema',
                'scale_hint', 'pixel_sizes', 'grid_spacing', 'coordinate_precision',
                'feature_density', 'resolution_source', 'resolution_borrowed_from_layer',
            }
            db_metadata = {k: v for k, v in gis_capabilities.items() if k in OGC_KEEP}
            db_metadata.update(source_record.extra)
            write_metadata_to_db(db_conn, identifier, url, db_metadata, status='success_ogc')
            stats_delta['processed'] += 1
            print(f"SUCCESS (OGC) - Layer: {db_metadata.get('layer_name')}")
            return stats_delta, None  # ← skip GDAL entirely

    except Exception as e:
        print(f"OGC/link check error: {e}")

    result_record = None
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
                'layer_count', 'geometry_type', 'layers',
                'title', 'doi', 'zenodo_id', 'filename', 'filesize',
            }
            db_metadata = {k: v for k, v in metadata.items() if k in AUGMENT_KEEP}
            write_metadata_to_db(db_conn, identifier, url, db_metadata, status='success')
            result_record = {'identifier': identifier, 'url': url, 'metadata': metadata}  # full version in output file
            stats_delta['processed'] += 1
        else:
            error_msg = result.get('error', 'Unknown error')
            if error_msg.startswith('Skipped:'):
                print(f"SKIPPED: {error_msg}")
                stats_delta['skipped'] += 1
                write_metadata_to_db(db_conn, identifier, url, {'reason': error_msg}, status=_skip_status(error_msg))
            else:
                error_msg = result.get('error', 'Unknown error')
                print(f"GDAL ERROR: {error_msg[:80]}")
                write_metadata_to_db(db_conn, identifier, url, {'error': error_msg}, status='gdal_error')
                stats_delta['errors'] += 1

        if output_f:
            output_record = {
                'identifier': identifier,
                'url': url,
                'metadata': result.get('metadata'),
                'date': datetime.now(timezone.utc).isoformat(),
                'process': 'spatial-extractor',
                'error': result.get('error') if not result['success'] else None
            }
            with output_lock:
                output_f.write(json.dumps(output_record, default=str) + '\n')
                output_f.flush()

    except Exception as e:
        print(f"EXCEPTION: {e}")
        write_metadata_to_db(db_conn, identifier, url, {'error': str(e)}, status='exception')
        stats_delta['errors'] += 1

    return stats_delta, result_record


def process_records(db_config: dict, adapter, output_file=None, limit=None, check_links=True,
                     max_workers: int = MAX_WORKERS):
    """
    Process records from any adapter (PostgreSQL, CSV, Zenodo), fanning work
    out across max_workers threads (see process_one_record).

    1. Iterates records from the given adapter
    2. Validates URLs with link checker (unless skipped)
    3. Extracts spatial metadata with GDAL
    4. Writes results to metadata.augments and metadata.augment_status
    """
    extractor = GDALMetadataExtractor()
    stats = {'processed': 0, 'valid_urls': 0, 'invalid_urls': 0, 'skipped': 0, 'errors': 0}
    results = []

    output_f = open(output_file, 'w') if output_file else None
    output_lock = threading.Lock()
    rows_processed = 0

    try:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = []
            for source_record in adapter:
                rows_processed += 1
                if limit and rows_processed > limit:
                    print(f"\nReached limit of {limit} records, stopping...")
                    break

                futures.append(executor.submit(
                    process_one_record, db_config, extractor, source_record,
                    rows_processed, check_links, output_f, output_lock
                ))

            for future in as_completed(futures):
                try:
                    stats_delta, result_record = future.result()
                except Exception as e:
                    print(f"Worker EXCEPTION: {e}")
                    stats_delta, result_record = {'errors': 1}, None

                for key, value in stats_delta.items():
                    stats[key] = stats.get(key, 0) + value
                if result_record:
                    results.append(result_record)

    finally:
        if output_f:
            output_f.close()
        close_thread_connections()

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
    threading.Thread(target=_watch_for_dump_flag, daemon=True).start()

    parser = argparse.ArgumentParser(description='Process spatial metadata from any source')

    # Only decide the source
    parser.add_argument('--source', choices=['postgresql', 'csv', 'zenodo'], default='postgresql', help='Source adapter to use')

    # CSV options
    parser.add_argument('--csv-file', help='Path to CSV file (required when --source=csv)')
    parser.add_argument('--csv-identifier-col', default='identifier', help='CSV column holding the record identifier')
    parser.add_argument('--csv-mediatype-col',  default='mediatype', help='CSV column holding the mediatype hint')

    # Zenodo options
    parser.add_argument('--zenodo-ids', nargs='+', help='Specific Zenodo record IDs')
    parser.add_argument('--zenodo-ids-csv', help='CSV file with a column of Zenodo DOIs/IDs to process')
    parser.add_argument('--zenodo-ids-col', default='identifier', help='Column in --zenodo-ids-csv holding the DOI/ID')
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
        adapter = get_adapter(
            'csv',
            filepath=args.csv_file,
            identifier_col=args.csv_identifier_col,
            mediatype_col=args.csv_mediatype_col,
        )

    elif args.source == 'zenodo':
        zenodo_ids = args.zenodo_ids
        if args.zenodo_ids_csv:
            zenodo_ids = read_zenodo_ids_from_csv(args.zenodo_ids_csv, args.zenodo_ids_col)
            print(f"Loaded {len(zenodo_ids)} Zenodo IDs from {args.zenodo_ids_csv}")

        if not zenodo_ids and not args.zenodo_query:
            print("--zenodo-ids, --zenodo-ids-csv, or --zenodo-query is required when --source=zenodo")
            exit(1)
        adapter = get_adapter(
            'zenodo',
            record_ids=zenodo_ids,
            search_query=args.zenodo_query,
            community=args.zenodo_community,
            access_token=args.zenodo_token,
            max_records=args.zenodo_max,
        )

    # Run
    process_records(DB_CONFIG, adapter, args.output, args.limit, check_links=not args.no_link_check)