
# -*- coding: utf-8 -*-


import sys
import requests
import pandas as pd

from utils.doi_check import extract as extract_dois
from tqdm import tqdm
import glob
import re

# ----------- CONFIG: fill these -----------


HEADERS = {
    "User-Agent": "VERM"
}

OUTFILE  = "SWR_copy/cited_by_results_{latest_index}.ndjson"
TIMEOUT  = 30.0
SLEEP_BETWEEN_CALLS = 0.5  # seconds
BASE_URL_WORKS = "https://api.crossref.org/works"

# ----------- Helpers -----------
 
def cleanup_link_df(df: pd.DataFrame, link_column='links'):
    df['DOI_list'] = df[link_column].apply(lambda x: extract_dois(x) if isinstance(x, str) else [])
    return df

def fetch_crossref_info(doi: str):
    url = f"{BASE_URL_WORKS}/{doi}"
    try:
        r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        r.raise_for_status()
        return r.json()
    
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            # sys.stderr.write(f"[WARN] DOI not found (404) for {doi}\n")
            return {"error": "404_not_found"}
        else:
            sys.stderr.write(f"[WARN] Crossref HTTP error for {doi}: {e}\n")
            return None
    except requests.RequestException as e:
        sys.stderr.write(f"[WARN] Crossref info request failed for {doi}: {e}\n")
        return None


def extract_reference_dois(ref):
    """
    ref: expected to be a list of dicts; each dict may contain a 'DOI' key.
    Returns a list of cleaned DOI strings.
    """
    if not isinstance(ref, list):
        return []

    dois = []
    for item in ref:
        if not isinstance(item, dict):
            continue

        val = item.get('DOI', item.get('doi'))

        if val is None:
            continue

        # coerce to string, strip whitespace
        s = str(val).strip()
        if not s:
            continue

        dois.append(s)

    # optional: deduplicate while preserving order
    seen = set()
    unique = []
    for d in dois:
        if d not in seen:
            seen.add(d)
            unique.append(d)

    return unique


def unique_preserve_order(seq):
    seen = set()
    out = []
    for x in seq:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out
def load_processed_results(pattern: str = "SWR_copy/cited_by_results_*.ndjson") -> set:
    """
    Read all cited_by_results_*.ndjson files and extract unique 'identifier' values.
    Returns a set of already-processed identifiers to skip and full dataset
    """
    processed_id = set()
    existing_files = glob.glob(pattern)
    data_processed_mining = pd.DataFrame()
    for f in existing_files:
        try:
            df = pd.read_json(f, orient="records", lines=True)
            df = df[df['res'].notna()]
            if 'identifier' in df.columns:
                processed_id.update(df['identifier'].dropna().unique())
            data_processed_mining = pd.concat([data_processed_mining, df], ignore_index=True)
        except Exception as e:
            print(f"Warning: Could not read {f}: {e}")
    
    return processed_id,data_processed_mining

# ----------- Main -----------
def main():

    df = pd.read_csv(r"SWR_copy\column.csv")


    # Rekening houden met reeds verwerkte bestanden

    pattern = r"cited_by_results_(\d+)\.ndjson"
    existing_files = glob.glob("SWR_copy/cited_by_results_*.ndjson")

    if existing_files:
        indices = []
        for f in existing_files:
            match = re.search(pattern, f)
            if match:
                indices.append(int(match.group(1)))
        
        if indices:
            next_idx = max(indices) + 1
    else:
        next_idx = 1
    
    processed_ids, df_DOI_processed = load_processed_results()
    df_DOI_processed = df_DOI_processed.drop(['cited_by_SWR_count','cited_by_identifiers','cited_by_dois'], axis=1)
    print(f"Skipping {len(processed_ids)} already-processed identifiers.")
    df = df[~df['identifier'].isin(processed_ids)].reset_index(drop=True)


    # Add column with extracted DOIs
    df = cleanup_link_df(df, link_column='links')
    df['count_DOIs'] = df['DOI_list'].apply(len)
    
    print('Stats for type of items and count DOIs')
    print(df.groupby(['type'])['count_DOIs'].value_counts())
    print('--------------------')
    print()

    df_DOI_mapping = df[['identifier','DOI_list']].copy()
    df_DOI_mapping = df_DOI_mapping.explode('DOI_list', ignore_index=True).rename(columns={'DOI_list': 'DOI'})
    df_DOI_mapping = df_DOI_mapping[df_DOI_mapping['DOI'].notna()]

    # ##### SUBSAMPLE TO TEST
    # df_DOI_mapping = df_DOI_mapping[:50]
    # #########################

    tqdm.pandas(desc="Crossref")
    df_DOI_mapping['res'] = df_DOI_mapping['DOI'].progress_apply(fetch_crossref_info)
    
    df_DOI_mapping['publisher'] = df_DOI_mapping['res'].apply(
        lambda x: x.get('message').get('publisher') if isinstance(x, dict) and 'error' not in x else None
    ) 
    df_DOI_mapping['ISSN'] = df_DOI_mapping['res'].apply(
        lambda x: x.get('message').get('ISSN') if isinstance(x, dict) and 'error' not in x else None
    )
    df_DOI_mapping['container-title'] = df_DOI_mapping['res'].apply(
        lambda x: x.get('message').get('container-title') if isinstance(x, dict) and 'error' not in x else None
    ) 
    df_DOI_mapping['is_cited_by_count'] = df_DOI_mapping['res'].apply(
        lambda x: x.get('message').get('is-referenced-by-count') if isinstance(x, dict) and 'error' not in x else None
    )
    df_DOI_mapping['reference_count'] = df_DOI_mapping['res'].apply(
        lambda x: x.get('message').get('reference-count') if isinstance(x, dict) and 'error' not in x else None
    )
    df_DOI_mapping['reference'] = df_DOI_mapping['res'].apply(
        lambda x: x.get('message').get('reference') if isinstance(x, dict) and 'error' not in x else None
    )
    df_DOI_mapping['reference_dois'] = df_DOI_mapping['reference'].apply(extract_reference_dois)


    df_DOI_mapping.to_json(OUTFILE.format(latest_index=next_idx), orient="records", lines=True, force_ascii=False)
    print(f"Done. Wrote {len(df_DOI_mapping)} citing links to {OUTFILE.format(latest_index=next_idx)}")

    df_DOI_Full= pd.concat([df_DOI_processed, df_DOI_mapping], ignore_index=True)


    df_ref_links = (
        df_DOI_Full[['identifier', 'DOI', 'reference_dois']]
        .explode('reference_dois', ignore_index=True)
        .rename(columns={'reference_dois': 'reference_DOI'})
    )
    
    agg = (
        df_ref_links.groupby('reference_DOI', as_index=False)
        .agg({
            'DOI': lambda s: unique_preserve_order(s.dropna().astype(str).str.strip()),
            'identifier': lambda s: unique_preserve_order(s.dropna().astype(str).str.strip()),
        })
        .rename(columns={
            'reference_DOI': 'DOI',
            'DOI': 'cited_by_dois',
            'identifier': 'cited_by_identifiers',
        })
    )

    agg['cited_by_SWR_count'] = agg['cited_by_dois'].apply(len)
    
   
    df_DOI_enriched = df_DOI_Full.merge(
        agg,
        on='DOI',
        how='left',
        #validate='one_to_one'  # sanity: df_DOI_mapping should have unique DOIs
    )

    df_DOI_enriched.to_json(OUTFILE.format(latest_index='latest'), orient="records", lines=True, force_ascii=False)
    print(f"Done. Wrote {len(df_DOI_enriched)} citing links to {OUTFILE.format(latest_index=next_idx)}")

if __name__ == "__main__":
    main()
