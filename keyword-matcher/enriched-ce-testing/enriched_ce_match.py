"""
Definition-enriched retrieve-and-rerank keyword matching (testing).

Same retrieval cascade as embedding_ce_match.py, but the vocabulary is built
directly from the SoilVoc skosmos TTL (not concepts.json) and each concept
carries an *enriched text*: "{primary_en_label}: {definition}" when the ttl has
a skos:definition, else just the label. The cross-encoder scores the subject
against that enriched text instead of the bare candidate label — mmarco is a
query->passage relevance model, so a short keyword vs. a label+definition
passage is closer to its training distribution than phrase vs. phrase.

Workflow:

    0. Vocabulary build (cached to enriched_concepts.json; delete to rebuild):
         - SPARQL over SoilVoc_skosmos.ttl for concepts, en pref/alt labels,
           definitions (blank node -> rdf:value) and exact/close match URIs.
         - For AgroVoc / ISO11074 match URIs, fetch multilingual labels
           (AgroVoc: remote SPARQL endpoint; ISO: local ../vocabs/ISO11074.ttl).
         - Attach enriched_text and has_definition to each concept.
    1. URL match        -> subject URI looked up in the concept match URIs.
    2. Exact label       -> subject label equals a concept label
                            (case-insensitive, any language).
    3. Retrieve + rerank -> otherwise:
         a. embed the subject label with the bi-encoder,
         b. top-k nearest concept labels by cosine (any language,
            >= COSINE_FLOOR), deduped to the best label per concept,
         c. cross-encode (subject_label, concept_enriched_text) per candidate,
         d. if the best CE score >= CE_THRESHOLD, apply that match.

Models:
    bi-encoder    : sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2
    cross-encoder : cross-encoder/mmarco-mMiniLMv2-L12-H384-v1  (sigmoid -> 0-1)

Retrieval stays on labels (the bi-encoder is a symmetric phrase model); only
the CE input changes vs. embedding_ce_match.py, so the candidate logs of the
two experiments are directly comparable per (subject, concept) pair.

Input  : SoilVoc_skosmos.ttl (vocab) + subjects.csv (id, uri, label, ...)
Output : enriched_ce_match_res.csv (matched pairs only), with has_definition
         flagging whether the CE passage actually contained a definition.

Note on thresholds: CE scores from label-only vs. label+definition passages
come from different distributions — calibrate them separately on a labelled
set (that is what the has_definition column in the candidate log is for).
CE_THRESHOLD = 0.60 is the usual placeholder.
"""

import csv
import hashlib
import json
import os
import re
import time

import numpy as np
import torch

from rdflib import Graph
from SPARQLWrapper import SPARQLWrapper, JSON
from sentence_transformers import SentenceTransformer, CrossEncoder, util

# --- config -----------------------------------------------------------------

HERE = os.path.dirname(os.path.abspath(__file__))
SOILVOC_TTL_PATH = os.path.join(HERE, "SoilVoc_skosmos.ttl")
ISO_TTL_PATH = os.path.join(HERE, "..", "vocabs", "ISO11074.ttl")
SUBJECTS_PATH = os.path.join(HERE, "subjects.csv")
OUTPUT_PATH = os.path.join(HERE, "enriched_ce_match_res.csv")
CONCEPTS_CACHE_PATH = os.path.join(HERE, "enriched_concepts.json")
EMB_CACHE_PATH = os.path.join(HERE, "concept_embeddings.npz")
CANDIDATES_LOG_PATH = os.path.join(HERE, "enriched_candidates_log.csv")

# When True, log every (subject, candidate) retrieval pair fed to the cross-
# encoder, with its cosine score and CE score, before best-per-subject
# selection. Raw material for threshold tuning (split on has_definition).
LOG_CANDIDATES = True

AGROVOC_ENDPOINT = "https://agrovoc.fao.org/sparql"
LANGS = ["en", "fr", "de", "it", "es", "nl", "pt"]

# The AgroVoc endpoint rate-limits (HTTP 429). Pace the requests and back off on
# failure; a build that loses labels to throttling is not cached (see below).
AGROVOC_DELAY_S = 0.5    # pause between requests
AGROVOC_RETRIES = 4      # attempts per URI, with exponential backoff

BI_MODEL_NAME = "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"
CE_MODEL_NAME = "cross-encoder/mmarco-mMiniLMv2-L12-H384-v1"

TOP_K = 15           # nearest concept labels retrieved per subject
COSINE_FLOOR = 0.40  # 0-1; drop retrieved candidates below this before reranking
CE_THRESHOLD = 0.60  # 0-1 (sigmoid of the mMiniLM relevance logit); recalibrate

# Passages are short (median 11 tokens, p99 115), so cap the CE input well below
# the model's 512: it bounds the padding cost per batch on this CPU-only box and
# truncates only a handful of long definitions.
CE_MAX_LENGTH = 128


# --- helpers ----------------------------------------------------------------

def format_string(input_string):
    """Strip a leading numeric prefix from a label (e.g. '106022 mikrobiologie')."""
    stripped = input_string.lstrip()
    if stripped and stripped[0].isdigit():
        return stripped.lstrip("0123456789").lstrip()
    return stripped


def clean_cell(value):
    """Treat empty strings and literal 'NULL' from the CSV as missing."""
    if value is None:
        return None
    value = value.strip()
    if value == "" or value.upper() == "NULL":
        return None
    return value


def all_labels(concept):
    """Flatten a concept's labels across all languages."""
    return [lab for labels in concept["labels"].values() for lab in labels]


def primary_label(concept):
    """English primary label if present, else the first available label."""
    en = concept["labels"].get("en")
    if en:
        return en[0]
    flat = all_labels(concept)
    return flat[0] if flat else concept["identifier"]


def url_match(cons, sub_uri):
    for c in cons:
        if sub_uri in c["uris"]:
            return c
    return None


def exact_label_match(cons, key_value):
    """Exact, case-insensitive label match against any language label."""
    key = format_string(key_value).lower()
    if not key:
        return None
    for c in cons:
        for lab in all_labels(c):
            if lab.lower() == key:
                return c
    return None


# --- vocabulary build (phase 0) ----------------------------------------------

def sparql_local(path, query):
    """Run a SPARQL query against a local ttl file; rows as {var: str}."""
    g = Graph()
    g.parse(path, format="ttl")
    results = g.query(query)
    return [
        {str(var): str(row[var]) for var in results.vars if row[var] is not None}
        for row in results
    ]


def fetch_agrovoc_labels(uri):
    """
    Fetch pref/alt labels for an AgroVoc URI.

    Returns (labels, ok). `ok` is False when every attempt failed — the caller
    uses it to avoid caching a vocabulary that silently lost labels to the
    endpoint's rate limiting.
    """
    query = f'''
    PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
    SELECT DISTINCT ?label
    WHERE {{
        {{ <{uri}> skos:prefLabel ?label }} UNION
        {{ <{uri}> skos:altLabel ?label }}
    }}
    '''
    bindings = None
    for attempt in range(AGROVOC_RETRIES):
        try:
            sparql = SPARQLWrapper(AGROVOC_ENDPOINT)
            sparql.setQuery(query)
            sparql.setReturnFormat(JSON)
            bindings = sparql.query().convert()["results"]["bindings"]
            break
        except Exception as e:
            if attempt == AGROVOC_RETRIES - 1:
                print(f"  WARNING: AgroVoc query failed for {uri}: {e}")
                return {}, False
            time.sleep(AGROVOC_DELAY_S * (2 ** attempt))  # back off and retry

    labels = {}
    for item in bindings:
        lang = item["label"].get("xml:lang", "")
        value = item["label"].get("value", "")
        if lang in LANGS and value:
            labels.setdefault(lang, []).append(value)
    return labels, True


def fetch_iso_labels(uri, iso_graph):
    """Fetch pref/alt labels for an ISO11074 URI from the local ttl graph."""
    query = f'''
    PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
    SELECT DISTINCT ?label ?lang
    WHERE {{
        {{ <{uri}> skos:prefLabel ?label . BIND(LANG(?label) AS ?lang) }} UNION
        {{ <{uri}> skos:altLabel ?label . BIND(LANG(?label) AS ?lang) }}
    }}
    '''
    labels = {}
    for row in iso_graph.query(query):
        lang = str(row["lang"]) if row["lang"] is not None else ""
        value = str(row["label"]) if row["label"] is not None else ""
        if lang in LANGS and value:
            labels.setdefault(lang, []).append(value)
    return labels


def merge_labels(base, extra):
    """Merge {lang: [labels]} dicts into base, deduping while keeping order."""
    for lang, labels in extra.items():
        merged = base.setdefault(lang, [])
        for lab in labels:
            if lab not in merged:
                merged.append(lab)
    return base


def clean_definition(text):
    """Collapse whitespace and drop a trailing 'Source: ...' citation tail."""
    text = " ".join(text.split())
    text = re.sub(r"\s*Source:\s.*$", "", text)
    return text.strip()


def build_concepts():
    """
    Build the enriched vocabulary from SoilVoc_skosmos.ttl:
    en pref/alt labels + definition from the ttl, multilingual labels fetched
    from AgroVoc (remote) and ISO11074 (local) via the exact/close match URIs,
    and an enriched_text ("{primary_en_label}: {definition}") per concept.
    """
    query = '''
    PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    SELECT ?concept ?label ?alt_label ?definition ?exact_match_uri ?close_match_uri
    WHERE {
        ?concept a skos:Concept ;
                 skos:prefLabel ?label .
        OPTIONAL { ?concept skos:altLabel ?alt_label }
        OPTIONAL {
            { ?concept skos:definition/rdf:value ?definition }
            UNION
            { ?concept skos:definition ?definition . FILTER(isLiteral(?definition)) }
        }
        OPTIONAL { ?concept skos:exactMatch ?exact_match_uri }
        OPTIONAL { ?concept skos:closeMatch ?close_match_uri }
    }
    '''
    rows = sparql_local(SOILVOC_TTL_PATH, query)
    print(f"SoilVoc query returned {len(rows)} rows")

    # Accumulate per concept; the optionals make the rows a cross product.
    by_id = {}
    for r in rows:
        cid = r.get("concept", "")
        if not cid:
            continue
        con = by_id.get(cid)
        if con is None:
            con = {
                "identifier": cid,
                "uris": [],
                "labels": {},
                "definition": None,
            }
            by_id[cid] = con
        for key in ("label", "alt_label"):
            lab = r.get(key, "")
            if lab and lab not in con["labels"].get("en", []):
                con["labels"].setdefault("en", []).append(lab)
        if con["definition"] is None and r.get("definition"):
            definition = clean_definition(r["definition"])
            if definition:
                con["definition"] = definition
        for key in ("exact_match_uri", "close_match_uri"):
            uri = r.get(key, "")
            if uri and uri not in con["uris"]:
                con["uris"].append(uri)

    cons = list(by_id.values())
    n_def = sum(1 for c in cons if c["definition"])
    print(f"Built {len(cons)} concepts ({n_def} with a definition)")

    # Multilingual label enrichment via the match URIs.
    iso_graph = None
    agro_uris = [u for c in cons for u in c["uris"]
                 if u.startswith("http://aims.fao.org/aos/agrovoc")]
    print(f"Fetching AgroVoc labels for {len(agro_uris)} URIs "
          f"(paced {AGROVOC_DELAY_S}s apart) ...")
    done = 0
    failed = 0
    for c in cons:
        for uri in c["uris"]:
            if uri.startswith("http://aims.fao.org/aos/agrovoc"):
                labels, ok = fetch_agrovoc_labels(uri)
                if ok:
                    merge_labels(c["labels"], labels)
                else:
                    failed += 1
                done += 1
                if done % 25 == 0:
                    print(f"  {done}/{len(agro_uris)} AgroVoc URIs done")
                time.sleep(AGROVOC_DELAY_S)
            elif uri.startswith("https://data.geoscience.earth/ncl/ISO11074"):
                if iso_graph is None:
                    iso_graph = Graph()
                    iso_graph.parse(ISO_TTL_PATH, format="ttl")
                merge_labels(c["labels"], fetch_iso_labels(uri, iso_graph))

    # Attach the CE passage text.
    for c in cons:
        label = primary_label(c)
        if c["definition"]:
            c["enriched_text"] = f"{label}: {c['definition']}"
            c["has_definition"] = True
        else:
            c["enriched_text"] = label
            c["has_definition"] = False

    return cons, failed


def load_or_build_concepts():
    """
    Load the enriched vocabulary cache, or build it from the ttl and cache it.

    A build that lost AgroVoc labels to the endpoint's rate limiting is NOT
    cached — caching it would silently freeze an incomplete vocabulary in place.
    The run continues on the partial labels; re-run to complete the build.
    """
    if os.path.exists(CONCEPTS_CACHE_PATH):
        with open(CONCEPTS_CACHE_PATH, "r", encoding="utf-8") as f:
            cons = json.load(f)
        print(f"Loaded {len(cons)} enriched concepts from cache "
              f"(delete {os.path.basename(CONCEPTS_CACHE_PATH)} to rebuild)")
        return cons

    cons, failed = build_concepts()
    if failed:
        print(f"  WARNING: {failed} AgroVoc URIs could not be fetched, so some "
              f"multilingual labels are missing. NOT caching this partial "
              f"vocabulary — re-run to retry those URIs.")
        return cons

    with open(CONCEPTS_CACHE_PATH, "w", encoding="utf-8") as f:
        json.dump(cons, f, ensure_ascii=False, indent=2)
    print(f"Cached enriched vocabulary to {CONCEPTS_CACHE_PATH}")
    return cons


# --- retrieval corpus ---------------------------------------------------------

def build_corpus(cons):
    """
    Flatten every concept label across all languages into a parallel corpus.

    Returns:
        corpus_labels : list[str]  - the label text
        corpus_concept_idx : list[int] - index into `cons` for each label
    """
    corpus_labels = []
    corpus_concept_idx = []
    for ci, c in enumerate(cons):
        for lab in all_labels(c):
            corpus_labels.append(lab)
            corpus_concept_idx.append(ci)
    return corpus_labels, corpus_concept_idx


def concepts_fingerprint(cons):
    """Stable hash of the concept labels, to invalidate the embedding cache."""
    h = hashlib.sha256()
    h.update(BI_MODEL_NAME.encode("utf-8"))
    for c in cons:
        h.update(c["identifier"].encode("utf-8"))
        for lab in all_labels(c):
            h.update(lab.encode("utf-8"))
    return h.hexdigest()


def load_or_build_concept_embeddings(bi_model, corpus_labels, fingerprint):
    """Embed concept labels once, cache to disk keyed by the fingerprint."""
    if os.path.exists(EMB_CACHE_PATH):
        cached = np.load(EMB_CACHE_PATH, allow_pickle=True)
        if str(cached["fingerprint"]) == fingerprint:
            print(f"Loaded cached concept embeddings ({len(corpus_labels)} labels)")
            return torch.from_numpy(cached["embeddings"])
        print("Embedding cache stale (vocabulary or model changed); rebuilding")

    print(f"Embedding {len(corpus_labels)} concept labels with the bi-encoder")
    emb = bi_model.encode(
        corpus_labels,
        convert_to_numpy=True,
        normalize_embeddings=True,
        show_progress_bar=True,
    )
    np.savez(EMB_CACHE_PATH, embeddings=emb, fingerprint=fingerprint)
    return torch.from_numpy(emb)


# --- main -------------------------------------------------------------------

def run():
    start_time = time.time()

    cons = load_or_build_concepts()

    with open(SUBJECTS_PATH, newline="", encoding="utf-8") as f:
        subjects = list(csv.DictReader(f))
    print(f"Loaded {len(subjects)} subjects")

    corpus_labels, corpus_concept_idx = build_corpus(cons)
    print(f"Concept corpus: {len(corpus_labels)} labels across all languages")

    # Load the bi-encoder and prepare (possibly cached) concept embeddings.
    bi_model = SentenceTransformer(BI_MODEL_NAME)
    fingerprint = concepts_fingerprint(cons)
    corpus_emb = load_or_build_concept_embeddings(
        bi_model, corpus_labels, fingerprint
    )

    results = [None] * len(subjects)

    # Subjects that fall through url/exact go to the retrieval stage. We collect
    # their labels so the bi-encoder can embed them in one batched call.
    retrieval_idx = []      # subject indices needing retrieval
    retrieval_labels = []   # their (raw) labels, parallel to retrieval_idx

    for i, sub in enumerate(subjects):
        sub_id = clean_cell(sub.get("id"))
        sub_uri = clean_cell(sub.get("uri"))
        sub_label = clean_cell(sub.get("label"))

        row = {
            "method": "no_match",
            "subject_label": sub_label,
            "vocab_label": None,
            "subject_id": sub_id,
            "subject_uri": sub_uri,
            "vocab_identifier": None,
            "cosine_score": None,
            "ce_score": None,
            "has_definition": None,
        }
        results[i] = row

        # 1. URL match
        if sub_uri is not None:
            matched = url_match(cons, sub_uri)
            if matched is not None:
                row["method"] = "url_match"
                row["vocab_identifier"] = matched["identifier"]
                row["vocab_label"] = primary_label(matched)
                continue

        # No label -> nothing more we can do
        if sub_label is None:
            continue

        # 2. Exact label match
        matched = exact_label_match(cons, sub_label)
        if matched is not None:
            row["method"] = "exact_match"
            row["vocab_identifier"] = matched["identifier"]
            row["vocab_label"] = primary_label(matched)
            continue

        # 3. Defer to embedding retrieval
        retrieval_idx.append(i)
        retrieval_labels.append(sub_label)

    print(f"Subjects needing retrieval + rerank: {len(retrieval_idx)}")

    # --- Retrieval stage: bi-encoder top-k over the concept labels ----------
    ce_pairs = []       # (subject_label, concept_enriched_text) for the reranker
    ce_pair_meta = []   # parallel (subject_idx, concept_idx, cosine, retrieval_label)

    if retrieval_idx:
        query_emb = bi_model.encode(
            retrieval_labels,
            convert_to_tensor=True,
            normalize_embeddings=True,
            show_progress_bar=True,
        )
        # Exact cosine top-k; corpus is small so no ANN index is needed.
        hits = util.semantic_search(
            query_emb, corpus_emb, top_k=TOP_K, score_function=util.cos_sim
        )

        # For each subject, keep the best-scoring label per concept, above floor.
        for q, subject_hits in enumerate(hits):
            subj_idx = retrieval_idx[q]
            subj_label = retrieval_labels[q]
            best_label_per_concept = {}  # concept_idx -> (cosine, label)
            for hit in subject_hits:
                cos = hit["score"]
                if cos < COSINE_FLOOR:
                    continue
                cidx = corpus_concept_idx[hit["corpus_id"]]
                label = corpus_labels[hit["corpus_id"]]
                cur = best_label_per_concept.get(cidx)
                if cur is None or cos > cur[0]:
                    best_label_per_concept[cidx] = (cos, label)
            for cidx, (cos, label) in best_label_per_concept.items():
                # The CE scores the subject against the concept's enriched
                # text, not the label that surfaced it in retrieval.
                ce_pairs.append((subj_label, cons[cidx]["enriched_text"]))
                ce_pair_meta.append((subj_idx, cidx, cos, label))

    print(f"Cross-encoder pairs to score: {len(ce_pairs)}")

    # --- Rerank stage: cross-encoder over the retrieved shortlist -----------
    if ce_pairs:
        ce_model = CrossEncoder(
            CE_MODEL_NAME,
            activation_fn=torch.nn.Sigmoid(),
            max_length=CE_MAX_LENGTH,
        )
        scores = ce_model.predict(ce_pairs, show_progress_bar=True)

        # Keep the single best-reranked candidate per subject. Optionally log
        # every scored pair (before this selection) for threshold tuning.
        best_by_subject = {}  # subj_idx -> (ce_score, concept_idx, cosine)
        candidate_log = []    # rows for CANDIDATES_LOG_PATH
        for (subj_idx, cidx, cos, ret_label), ce_score in zip(ce_pair_meta, scores):
            ce_score = float(ce_score)
            cur = best_by_subject.get(subj_idx)
            if cur is None or ce_score > cur[0]:
                best_by_subject[subj_idx] = (ce_score, cidx, cos)
            if LOG_CANDIDATES:
                concept = cons[cidx]
                candidate_log.append({
                    "subject_id": results[subj_idx]["subject_id"],
                    "subject_label": results[subj_idx]["subject_label"],
                    "candidate_vocab_identifier": concept["identifier"],
                    "candidate_vocab_label": primary_label(concept),
                    "matched_label": ret_label,
                    "has_definition": concept["has_definition"],
                    "cosine_score": round(cos, 4),
                    "ce_score": round(ce_score, 4),
                })

        if LOG_CANDIDATES and candidate_log:
            log_fieldnames = [
                "subject_id", "subject_label", "candidate_vocab_identifier",
                "candidate_vocab_label", "matched_label", "has_definition",
                "cosine_score", "ce_score",
            ]
            with open(CANDIDATES_LOG_PATH, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=log_fieldnames, quoting=csv.QUOTE_ALL)
                writer.writeheader()
                writer.writerows(candidate_log)
            print(f"Wrote {len(candidate_log)} candidate pairs to {CANDIDATES_LOG_PATH}")

        for subj_idx, (ce_score, cidx, cos) in best_by_subject.items():
            row = results[subj_idx]
            # Record the best retrieval/CE seen even below threshold (for tuning).
            row["cosine_score"] = round(cos, 4)
            row["ce_score"] = round(ce_score, 4)
            row["has_definition"] = cons[cidx]["has_definition"]
            if ce_score >= CE_THRESHOLD:
                concept = cons[cidx]
                row["method"] = "enriched_cross_encoder"
                row["vocab_identifier"] = concept["identifier"]
                row["vocab_label"] = primary_label(concept)

    # --- Write output: only enriched-CE matches (exclude url/exact/no_match) --
    # The method column is dropped since every row is an enriched_cross_encoder match.
    matched_rows = [r for r in results if r["method"] == "enriched_cross_encoder"]
    fieldnames = [
        "subject_label", "vocab_label", "subject_id", "subject_uri",
        "vocab_identifier", "cosine_score", "ce_score", "has_definition",
    ]
    with open(OUTPUT_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f, fieldnames=fieldnames, quoting=csv.QUOTE_ALL, extrasaction="ignore"
        )
        writer.writeheader()
        writer.writerows(matched_rows)

    print(f"\nWrote {len(matched_rows)} enriched_cross_encoder matches "
          f"(of {len(results)} subjects) to {OUTPUT_PATH}")

    elapsed_min = (time.time() - start_time) / 60
    print(f"Total execution time: {elapsed_min:.2f} minutes")


if __name__ == "__main__":
    run()
