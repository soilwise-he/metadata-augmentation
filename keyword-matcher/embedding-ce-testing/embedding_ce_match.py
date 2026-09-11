"""
Retrieve-and-rerank keyword matching (testing).

Same cascade as fuzzy_ce_match.py, but the fuzzy candidate gate is replaced by
bi-encoder embedding retrieval, so candidates are found by *meaning* instead of
by spelling. The cross-encoder then reranks that shortlist.

Workflow per subject:

    1. URL match       -> if the subject has a URI, look it up in concept URIs.
    2. Exact label      -> otherwise, if the subject label equals a concept label
                          (case-insensitive, any language), take it.
    3. Retrieve + rerank -> otherwise:
         a. embed the subject label with the bi-encoder,
         b. take the top-k nearest concept labels by cosine (>= COSINE_FLOOR),
         c. rerank each (subject_label, candidate_label) pair with the
            cross-encoder,
         d. if the best CE score >= CE_THRESHOLD, apply that match.

Models:
    bi-encoder    : sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2
    cross-encoder : cross-encoder/mmarco-mMiniLMv2-L12-H384-v1  (sigmoid -> 0-1)

Both are multilingual, covering the en/de/nl/it/fr/es labels in concepts.json.

Input  : concepts.json (vocab) + subjects.csv (id, uri, label, ...)
Output : embedding_ce_match_res.csv  (matched pairs only) with the chosen match,
         the method, the retrieval cosine score and the cross-encoder score.

Note on thresholds: cosine is on a 0-1 scale (COSINE_FLOOR just prunes obvious
non-matches before reranking). The cross-encoder emits a relevance logit; a
Sigmoid maps it to 0-1 and CE_THRESHOLD is the accept cutoff. CE_THRESHOLD is a
placeholder to re-calibrate on labelled data. Constants are below.
"""

import csv
import hashlib
import json
import os
import time

import numpy as np
import torch

from sentence_transformers import SentenceTransformer, CrossEncoder, util

# --- config -----------------------------------------------------------------

HERE = os.path.dirname(os.path.abspath(__file__))
CONCEPTS_PATH = os.path.join(HERE, "..", "concepts.json")
SUBJECTS_PATH = os.path.join(HERE, "subjects.csv")
OUTPUT_PATH = os.path.join(HERE, "embedding_ce_match_res.csv")
CACHE_PATH = os.path.join(HERE, "concept_embeddings.npz")
CANDIDATES_LOG_PATH = os.path.join(HERE, "embedding_candidates_log.csv")

# When True, log every (subject, candidate) retrieval pair fed to the cross-encoder,
# with its cosine score and CE score, before best-per-subject selection. This is
# the raw retrieval output (one row per candidate), useful for threshold tuning.
LOG_CANDIDATES = True

BI_MODEL_NAME = "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"
CE_MODEL_NAME = "cross-encoder/mmarco-mMiniLMv2-L12-H384-v1"

TOP_K = 15           # nearest concept labels retrieved per subject
COSINE_FLOOR = 0.40  # 0-1; drop retrieved candidates below this before reranking
CE_THRESHOLD = 0.60  # 0-1 (sigmoid of the mMiniLM relevance logit); recalibrate


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


def build_corpus(cons):
    """
    Flatten every concept label across all languages into a parallel corpus.

    Returns:
        corpus_labels : list[str]  - the label text (as-is, for reranking)
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
    if os.path.exists(CACHE_PATH):
        cached = np.load(CACHE_PATH, allow_pickle=True)
        if str(cached["fingerprint"]) == fingerprint:
            print(f"Loaded cached concept embeddings ({len(corpus_labels)} labels)")
            return torch.from_numpy(cached["embeddings"])
        print("Concept cache stale (concepts.json or model changed); rebuilding")

    print(f"Embedding {len(corpus_labels)} concept labels with the bi-encoder")
    emb = bi_model.encode(
        corpus_labels,
        convert_to_numpy=True,
        normalize_embeddings=True,
        show_progress_bar=True,
    )
    np.savez(CACHE_PATH, embeddings=emb, fingerprint=fingerprint)
    return torch.from_numpy(emb)


# --- main -------------------------------------------------------------------

def run():
    start_time = time.time()

    with open(CONCEPTS_PATH, "r", encoding="utf-8") as f:
        cons = json.load(f)
    print(f"Loaded {len(cons)} concepts")

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

    # --- Retrieval stage: bi-encoder top-k over the concept corpus ----------
    ce_pairs = []       # (subject_label, candidate_label) for the reranker
    ce_pair_meta = []   # parallel (subject_idx, concept_idx, cosine_score)

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
                ce_pairs.append((subj_label, label))
                ce_pair_meta.append((subj_idx, cidx, cos))

    print(f"Cross-encoder pairs to score: {len(ce_pairs)}")

    # --- Rerank stage: cross-encoder over the retrieved shortlist -----------
    if ce_pairs:
        ce_model = CrossEncoder(CE_MODEL_NAME, activation_fn=torch.nn.Sigmoid())
        scores = ce_model.predict(ce_pairs, show_progress_bar=True)

        # Keep the single best-reranked candidate per subject. Optionally log
        # every scored pair (before this selection) for threshold tuning.
        best_by_subject = {}  # subj_idx -> (ce_score, concept_idx, cosine)
        candidate_log = []    # rows for CANDIDATES_LOG_PATH
        for (subj_idx, cidx, cos), (pair, ce_score) in zip(ce_pair_meta, zip(ce_pairs, scores)):
            ce_score = float(ce_score)
            cur = best_by_subject.get(subj_idx)
            if cur is None or ce_score > cur[0]:
                best_by_subject[subj_idx] = (ce_score, cidx, cos)
            if LOG_CANDIDATES:
                subj_label, matched_label = pair
                concept = cons[cidx]
                candidate_log.append({
                    "subject_id": results[subj_idx]["subject_id"],
                    "subject_label": subj_label,
                    "candidate_vocab_identifier": concept["identifier"],
                    "candidate_vocab_label": primary_label(concept),
                    "matched_label": matched_label,
                    "cosine_score": round(cos, 4),
                    "ce_score": round(ce_score, 4),
                })

        if LOG_CANDIDATES and candidate_log:
            log_fieldnames = [
                "subject_id", "subject_label", "candidate_vocab_identifier",
                "candidate_vocab_label", "matched_label", "cosine_score", "ce_score",
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
            if ce_score >= CE_THRESHOLD:
                concept = cons[cidx]
                row["method"] = "embedding_cross_encoder"
                row["vocab_identifier"] = concept["identifier"]
                row["vocab_label"] = primary_label(concept)

    # --- Write output: only embedding+CE matches (exclude url/exact/no_match) --
    # The method column is dropped since every row is an embedding_cross_encoder match.
    matched_rows = [r for r in results if r["method"] == "embedding_cross_encoder"]
    fieldnames = [
        "subject_label", "vocab_label", "subject_id",
        "subject_uri", "vocab_identifier", "cosine_score", "ce_score",
    ]
    with open(OUTPUT_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f, fieldnames=fieldnames, quoting=csv.QUOTE_ALL, extrasaction="ignore"
        )
        writer.writeheader()
        writer.writerows(matched_rows)

    print(f"\nWrote {len(matched_rows)} embedding_cross_encoder matches "
          f"(of {len(results)} subjects) to {OUTPUT_PATH}")

    elapsed_min = (time.time() - start_time) / 60
    print(f"Total execution time: {elapsed_min:.2f} minutes")


if __name__ == "__main__":
    run()
