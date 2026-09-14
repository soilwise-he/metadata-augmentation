"""
Combined fuzzy-match + cross-encoder keyword matching (testing).

Workflow per subject (mirrors match.py, then adds a semantic re-ranking step):

    1. URL match      -> if the subject has a URI, look it up in the concept URIs.
    2. Exact label    -> otherwise, if the subject label equals a concept label
                         (case-insensitive, any language), take it.
    3. Fuzzy + CE     -> otherwise, collect every concept whose best label fuzz
                         ratio >= FUZZY_THRESHOLD (60). Run a cross-encoder on each
                         (subject_label, candidate_label) pair. If the highest
                         cross-encoder score >= CE_THRESHOLD (0.60), apply that match.

Input  : concepts.json (vocab) + subjects.csv (id, uri, label, ...)
Output : fuzzy_ce_match_res.csv with the chosen match plus which method produced
         it, the fuzzy score and the cross-encoder score.

Note on thresholds: fuzz.ratio is on a 0-100 scale, so FUZZY_THRESHOLD = 60.
The reranker (cross-encoder/mmarco-mMiniLMv2-L12-H384-v1) is multilingual and
emits a relevance logit; a Sigmoid maps it to 0-1, and CE_THRESHOLD = 0.60 is a
placeholder to re-calibrate on labelled data. Both constants are below.
"""

import csv
import json
import os
import time

import torch

from thefuzz import fuzz
from sentence_transformers import CrossEncoder

# --- config -----------------------------------------------------------------

HERE = os.path.dirname(os.path.abspath(__file__))
CONCEPTS_PATH = os.path.join(HERE, "..", "concepts.json")
SUBJECTS_PATH = os.path.join(HERE, "subjects.csv")
OUTPUT_PATH = os.path.join(HERE, "fuzzy_ce_match_res.csv")
CANDIDATES_LOG_PATH = os.path.join(HERE, "fuzzy_candidates_log.csv")

# When True, log every (subject, candidate) fuzzy pair fed to the cross-encoder,
# with its fuzzy score and CE score, before best-per-subject selection. This is
# the raw fuzzy-gate output (one row per candidate), useful for threshold tuning.
LOG_CANDIDATES = True

CE_MODEL_NAME = "cross-encoder/mmarco-mMiniLMv2-L12-H384-v1"

FUZZY_THRESHOLD = 60   # 0-100 (fuzz.ratio)
# This multilingual reranker emits a relevance logit; a Sigmoid activation (set
# on the CrossEncoder below) maps it to 0-1. The threshold is a placeholder that
# should be re-calibrated on a labelled set rather than carried over from stsb.
CE_THRESHOLD = 0.60    # 0-1 (sigmoid of the mMiniLM relevance logit)


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


def fuzzy_candidates(cons, key_value, threshold=FUZZY_THRESHOLD):
    """
    Return a list of (concept, best_fuzzy_score, best_matching_label) for every
    concept whose highest label fuzz ratio meets the threshold.
    """
    key = format_string(key_value).lower()
    candidates = []
    for c in cons:
        best_score = 0
        best_label = None
        for lab in all_labels(c):
            ratio = fuzz.ratio(key, lab.lower())
            if ratio > best_score:
                best_score = ratio
                best_label = lab
        if best_score >= threshold:
            candidates.append((c, best_score, best_label))
    return candidates


# --- main -------------------------------------------------------------------

def run():
    start_time = time.time()

    with open(CONCEPTS_PATH, "r", encoding="utf-8") as f:
        cons = json.load(f)
    print(f"Loaded {len(cons)} concepts")

    with open(SUBJECTS_PATH, newline="", encoding="utf-8") as f:
        subjects = list(csv.DictReader(f))
    print(f"Loaded {len(subjects)} subjects")

    # Result rows keyed by subject index; CE work is deferred and batched.
    results = [None] * len(subjects)

    # For the fuzzy/CE stage we gather every (subject, candidate) pair so the
    # cross-encoder can score them all in a single batched predict() call.
    ce_pairs = []          # list of (a, b) sentence pairs
    ce_pair_meta = []      # parallel list of (subject_idx, concept, fuzzy_score)

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
            "fuzzy_score": None,
            "ce_score": None,
        }

        # 1. URL match
        if sub_uri is not None:
            matched = url_match(cons, sub_uri)
            if matched is not None:
                row["method"] = "url_match"
                row["vocab_identifier"] = matched["identifier"]
                row["vocab_label"] = primary_label(matched)
                results[i] = row
                continue

        # No label -> nothing more we can do
        if sub_label is None:
            results[i] = row
            continue

        # 2. Exact label match
        matched = exact_label_match(cons, sub_label)
        if matched is not None:
            row["method"] = "exact_match"
            row["vocab_identifier"] = matched["identifier"]
            row["vocab_label"] = primary_label(matched)
            results[i] = row
            continue

        # 3. Fuzzy candidates -> queue for cross-encoder scoring
        candidates = fuzzy_candidates(cons, sub_label)
        results[i] = row  # default: no_match unless a CE candidate clears the bar
        for concept, fscore, flabel in candidates:
            ce_pairs.append((sub_label, flabel))
            ce_pair_meta.append((i, concept, fscore))

    print(f"Cross-encoder pairs to score: {len(ce_pairs)} "
          f"(across subjects needing fuzzy+CE)")

    # Batched cross-encoder scoring
    if ce_pairs:
        model = CrossEncoder(CE_MODEL_NAME, activation_fn=torch.nn.Sigmoid())
        scores = model.predict(ce_pairs, show_progress_bar=True)

        # For each subject keep the single best-scoring candidate. Optionally
        # log every scored pair (before this selection) for threshold tuning.
        best_by_subject = {}  # idx -> (ce_score, concept, fuzzy_score)
        candidate_log = []    # rows for CANDIDATES_LOG_PATH
        for (idx, concept, fscore), (pair, ce_score) in zip(ce_pair_meta, zip(ce_pairs, scores)):
            ce_score = float(ce_score)
            cur = best_by_subject.get(idx)
            if cur is None or ce_score > cur[0]:
                best_by_subject[idx] = (ce_score, concept, fscore)
            if LOG_CANDIDATES:
                sub_label, matched_label = pair
                candidate_log.append({
                    "subject_id": results[idx]["subject_id"],
                    "subject_label": sub_label,
                    "candidate_vocab_identifier": concept["identifier"],
                    "candidate_vocab_label": primary_label(concept),
                    "matched_label": matched_label,
                    "fuzzy_score": fscore,
                    "ce_score": round(ce_score, 4),
                })

        if LOG_CANDIDATES and candidate_log:
            log_fieldnames = [
                "subject_id", "subject_label", "candidate_vocab_identifier",
                "candidate_vocab_label", "matched_label", "fuzzy_score", "ce_score",
            ]
            with open(CANDIDATES_LOG_PATH, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=log_fieldnames, quoting=csv.QUOTE_ALL)
                writer.writeheader()
                writer.writerows(candidate_log)
            print(f"Wrote {len(candidate_log)} candidate pairs to {CANDIDATES_LOG_PATH}")

        for idx, (ce_score, concept, fscore) in best_by_subject.items():
            row = results[idx]
            # Record the best fuzzy/CE seen even when below threshold (for tuning)
            row["fuzzy_score"] = fscore
            row["ce_score"] = round(ce_score, 4)
            if ce_score >= CE_THRESHOLD:
                row["method"] = "fuzzy_cross_encoder"
                row["vocab_identifier"] = concept["identifier"]
                row["vocab_label"] = primary_label(concept)

    # Write output: only fuzzy+CE matches (exclude url_match/exact_match/no_match).
    # The method column is dropped since every row is a fuzzy_cross_encoder match.
    matched_rows = [r for r in results if r["method"] == "fuzzy_cross_encoder"]
    fieldnames = [
        "subject_label", "vocab_label", "subject_id",
        "subject_uri", "vocab_identifier", "fuzzy_score", "ce_score",
    ]
    with open(OUTPUT_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f, fieldnames=fieldnames, quoting=csv.QUOTE_ALL, extrasaction="ignore"
        )
        writer.writeheader()
        writer.writerows(matched_rows)

    print(f"\nWrote {len(matched_rows)} fuzzy_cross_encoder matches "
          f"(of {len(results)} subjects) to {OUTPUT_PATH}")

    elapsed_min = (time.time() - start_time) / 60
    print(f"Total execution time: {elapsed_min:.2f} minutes")


if __name__ == "__main__":
    run()
