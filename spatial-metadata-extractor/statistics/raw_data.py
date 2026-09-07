"""
load_raw_data.py
-----------------
Loads a CSV with columns (id, record_id, name, format, url, description)
into a new metadata.raw_data table in Postgres — a straight, unmodified
copy of the source CSV, kept separate from the processed
metadata.records / metadata.augments tables.

Usage:
    python load_raw_data.py path/to/file.csv

Install:
    pip install psycopg2-binary python-dotenv --break-system-packages
"""

import os
import sys
import csv
from urllib.parse import unquote

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    'host':     os.getenv('DB_HOST', 'localhost'),
    'port':     int(os.getenv('DB_PORT', 5432)),
    'dbname':   os.getenv('DB_NAME', 'postgres'),
    'user':     os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', 'w4qu+0sj'),
}

CREATE_TABLE_SQL = """
    CREATE SCHEMA IF NOT EXISTS metadata;

    CREATE TABLE IF NOT EXISTS metadata.raw_data (
        id           bigserial PRIMARY KEY,
        source_id    text,
        record_id    text,
        name         text,
        format       text,
        url          text,
        description  text,
        loaded_at    timestamptz NOT NULL DEFAULT now()
    );
"""

INSERT_SQL = """
    INSERT INTO metadata.raw_data (source_id, record_id, name, format, url, description)
    VALUES (%s, %s, %s, %s, %s, %s)
"""


def load_csv(filepath: str, identifier_col: str = "record_id", mediatype_col: str = "format"):
    """
    Read the CSV, decode the percent-encoded url field, return a list of row tuples.

    identifier_col / mediatype_col let this handle either column naming
    convention seen across different exports, e.g.:
      - id, record_id, name, format, url, description
      - id, identifier, name, mediatype, url, description
    """
    rows = []
    skipped = 0

    with open(filepath, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        fieldnames = reader.fieldnames or []

        required_cols = {"id", identifier_col, "name", mediatype_col, "url", "description"}
        missing = required_cols - set(fieldnames)
        if missing:
            raise ValueError(
                f"CSV is missing expected columns: {missing}. Found: {fieldnames}. "
                f"If your file uses different column names, pass identifier_col/mediatype_col explicitly."
            )

        for i, row in enumerate(reader, 1):
            url_raw = (row.get("url") or "").strip()
            if not url_raw:
                skipped += 1
                continue

            url_decoded = unquote(url_raw)

            rows.append((
                row.get("id", "").strip() or None,
                row.get(identifier_col, "").strip() or None,
                row.get("name", "").strip() or None,
                row.get(mediatype_col, "").strip() or None,
                url_decoded,
                row.get("description", "").strip() or None,
            ))

    print(f"Read {len(rows)} usable rows, skipped {skipped} blank/empty rows")
    return rows


def main():
    if len(sys.argv) < 2:
        print("Usage: python load_raw_data.py path/to/file.csv [identifier_col] [mediatype_col]")
        print("  Defaults: identifier_col=record_id, mediatype_col=format")
        sys.exit(1)

    filepath = sys.argv[1]
    identifier_col = sys.argv[2] if len(sys.argv) > 2 else "record_id"
    mediatype_col = sys.argv[3] if len(sys.argv) > 3 else "format"

    rows = load_csv(filepath, identifier_col=identifier_col, mediatype_col=mediatype_col)

    if not rows:
        print("Nothing to load.")
        return

    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            cur.execute(CREATE_TABLE_SQL)
            conn.commit()
            print("Ensured metadata.raw_data table exists")

            psycopg2.extras.execute_batch(cur, INSERT_SQL, rows, page_size=500)
            conn.commit()
            print(f"Inserted {len(rows)} rows into metadata.raw_data")

            cur.execute("SELECT COUNT(*) FROM metadata.raw_data")
            total = cur.fetchone()[0]
            print(f"Table now contains {total} total rows")

    except Exception as e:
        conn.rollback()
        print(f"Error loading data: {e}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()