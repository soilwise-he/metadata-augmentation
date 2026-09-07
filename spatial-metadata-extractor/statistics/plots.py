"""
unmatched_layers_stats.py
--------------------------
Focuses specifically on records where the OGC service responded but no
layer could be matched (layer_name is null, bbox/title/abstract empty).

For these "connected but empty" records, the only useful signal left is
the original CSV-derived fields: name and description. This script:

  1. Counts how many records fall into this "unmatched" bucket
  2. Extracts the most common words/phrases from `name` and `description`
  3. Breaks down which service_types (wms/wfs/wcs/...) have the most
     unmatched layers — tells you where the matching logic struggles most
  4. Looks for patterns in `name` that might explain WHY matching failed
     (e.g. names with a workspace prefix like "geonetwork:something")

Install:
    pip install psycopg2-binary pandas matplotlib python-dotenv --break-system-packages

Usage:
    python unmatched_layers_stats.py
"""

import os
import json
import re
from collections import Counter

import psycopg2
import psycopg2.extras
import pandas as pd
import matplotlib.pyplot as plt
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    'host':     os.getenv('DB_HOST', 'localhost'),
    'port':     int(os.getenv('DB_PORT', 5432)),
    'dbname':   os.getenv('DB_NAME', 'postgres'),
    'user':     os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', ''),
}

# Common English/Dutch/French stopwords — extend as needed for your catalogue's languages
STOPWORDS = {
    'the', 'a', 'an', 'of', 'for', 'and', 'or', 'in', 'on', 'at', 'to', 'with',
    'is', 'are', 'was', 'were', 'be', 'this', 'that', 'these', 'those',
    'de', 'het', 'van', 'voor', 'en', 'la', 'le', 'les', 'des', 'du', 'et',
    'data', 'service', 'layer', 'dataset', 'map', 'wms', 'wfs', 'null',
}


def fetch_unmatched_layers(conn):
    """
    Pull records where an OGC service was detected but no layer was matched.
    'Unmatched' = service_type present AND layer_name is null/missing.
    """
    query = """
        SELECT record_id, metadata
        FROM metadata.augments
        WHERE process = 'spatial-extractor'
          AND metadata ? 'service_type'
          AND (
                NOT (metadata ? 'layer_name')
                OR metadata->'layer_name' = 'null'::jsonb
          )
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(query)
        return cur.fetchall()


def fetch_matched_layers(conn):
    """For comparison: records where a layer WAS matched successfully."""
    query = """
        SELECT record_id, metadata
        FROM metadata.augments
        WHERE process = 'spatial-extractor'
          AND metadata ? 'service_type'
          AND metadata ? 'layer_name'
          AND metadata->'layer_name' != 'null'::jsonb
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(query)
        return cur.fetchall()


def parse_metadata(row):
    meta = row['metadata']
    if isinstance(meta, str):
        try:
            meta = json.loads(meta)
        except (json.JSONDecodeError, TypeError):
            return None
    return meta if isinstance(meta, dict) else None


def tokenize(text):
    """Lowercase, strip punctuation, split into words, drop stopwords/short tokens."""
    if not text:
        return []
    text = text.lower()
    text = re.sub(r'[^a-z0-9\u00C0-\u017F\s]', ' ', text)  # keep accented Latin chars
    tokens = text.split()
    return [t for t in tokens if len(t) > 2 and t not in STOPWORDS]


def analyze_name_patterns(names):
    """
    Look for structural patterns in the `name` field that might explain
    why exact-match layer lookup failed — e.g. a workspace prefix
    ("workspace:layername") that the live WMS capabilities doesn't expose
    the same way.
    """
    has_colon = sum(1 for n in names if n and ':' in n)
    has_space = sum(1 for n in names if n and ' ' in n)
    starts_with_colon = sum(1 for n in names if n and n.strip().startswith(':'))
    total = len([n for n in names if n])

    return {
        'total_with_name': total,
        'contains_colon': has_colon,
        'starts_with_colon': starts_with_colon,
        'contains_space': has_space,
    }


def main():
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        unmatched_rows = fetch_unmatched_layers(conn)
        matched_rows = fetch_matched_layers(conn)
    finally:
        conn.close()

    print(f"Unmatched-layer records: {len(unmatched_rows)}")
    print(f"Matched-layer records:   {len(matched_rows)}")
    if unmatched_rows or matched_rows:
        total = len(unmatched_rows) + len(matched_rows)
        print(f"Unmatched rate: {100 * len(unmatched_rows) / total:.1f}%")

    unmatched_meta = [parse_metadata(r) for r in unmatched_rows]
    unmatched_meta = [m for m in unmatched_meta if m is not None]

    # --- Word frequency from name + description ---
    word_counter = Counter()
    names, descriptions = [], []
    for m in unmatched_meta:
        name = m.get('name')
        desc = m.get('description')
        names.append(name)
        descriptions.append(desc)
        word_counter.update(tokenize(name))
        word_counter.update(tokenize(desc))

    print("\nTop 25 words in name/description of unmatched records:")
    for word, count in word_counter.most_common(25):
        print(f"  {word:20s} {count}")

    # --- service_type breakdown for unmatched ---
    service_counter = Counter(m.get('service_type') for m in unmatched_meta if m.get('service_type'))
    print("\nUnmatched records by service_type:")
    for svc, count in service_counter.most_common():
        print(f"  {svc:10s} {count}")

    # --- Name pattern analysis ---
    pattern_stats = analyze_name_patterns(names)
    print("\nName field patterns (possible clues to matching failures):")
    for k, v in pattern_stats.items():
        print(f"  {k:20s} {v}")

    # --- Save full word list + raw unmatched table to CSV for your own digging ---
    df = pd.DataFrame({
        'record_id': [r['record_id'] for r in unmatched_rows],
        'name': names,
        'description': descriptions,
        'service_type': [m.get('service_type') for m in unmatched_meta],
    })
    df.to_csv("unmatched_layers.csv", index=False)
    print(f"\nSaved {len(df)} unmatched records to unmatched_layers.csv")

    # --- Plot: word frequency + service_type breakdown side by side ---
    fig, axes = plt.subplots(1, 2, figsize=(16, 7))

    top_words = word_counter.most_common(20)
    if top_words:
        labels, counts = zip(*top_words)
        axes[0].barh(range(len(labels)), counts, color='#3b6ea5')
        axes[0].set_yticks(range(len(labels)))
        axes[0].set_yticklabels(labels, fontsize=9)
        axes[0].invert_yaxis()
        axes[0].set_xlabel("Occurrences")
        axes[0].set_title("Most common words\n(name + description of unmatched records)")
    else:
        axes[0].text(0.5, 0.5, "No words found", ha='center', va='center')
        axes[0].set_axis_off()

    if service_counter:
        labels, counts = zip(*service_counter.most_common())
        axes[1].bar(labels, counts, color='#e67e22')
        axes[1].set_ylabel("Unmatched record count")
        axes[1].set_title("Unmatched layers by service type")
        for i, c in enumerate(counts):
            axes[1].text(i, c, str(c), ha='center', va='bottom')
    else:
        axes[1].text(0.5, 0.5, "No service_type data", ha='center', va='center')
        axes[1].set_axis_off()

    plt.tight_layout()
    plt.savefig("unmatched_layers_stats.png", dpi=150, bbox_inches='tight')
    print("Saved plot to unmatched_layers_stats.png")
    plt.show()


if __name__ == "__main__":
    main()