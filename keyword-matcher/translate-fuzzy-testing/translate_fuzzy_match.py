"""
Translation-enriched fuzzy keyword matching (testing).

Idea: instead of a semantic model, widen the vocabulary itself. Most concepts in
concepts.json only carry an English prefLabel (AGROVOC/ISO supply other-language
labels for ~13% of them). Here we machine-translate the English label into the
target languages via the DeepL API to fill the *missing* language slots, then run
the same cheap cascade as the other experiments — no cross-encoder, just fuzzy.

Workflow per subject (mirrors match.py):

    1. URL match    -> if the subject has a URI, look it up in the concept URIs.
    2. Exact label  -> otherwise, if the subject label equals a concept label
                       (case-insensitive, any language), take it.
    3. Fuzzy match  -> otherwise, score the subject label against every concept
                       label (fuzz.ratio, all languages incl. the translated
                       ones). If the best score >= FUZZY_THRESHOLD (80), apply it.
                       Subjects of <= 4 characters skip this stage: fuzz.ratio is
                       unreliable on strings that short (oil/soil scores 86).

Enrichment rules:
    - Source is the English prefLabel (primary English label).
    - Target languages: en, fr, de, it, es, nl, pt. English is the source, so it
      is never translated; a target language is only filled when the concept has
      NO existing label for it. Curated AGROVOC/ISO labels are never overwritten.
    - concepts.json is NOT rewritten; enrichment lives only in memory for this run.
    - Translations are cached to translations_cache.json so re-runs don't re-spend
      DeepL quota. Set DEEPL_AUTH_KEY in the environment to call the API; without
      it the script runs on whatever is already cached and warns about the rest.

Input  : ../concepts.json (vocab) + subjects.csv (id, uri, label, ...)
Output : translate_fuzzy_match_res.csv — fuzzy matches only, with the (possibly
         translated) label and its language that produced the match.

Note on the threshold: fuzz.ratio is on a 0-100 scale; FUZZY_THRESHOLD = 80 is a
placeholder to re-calibrate on a labelled set, not a validated value.
"""

import csv
import json
import os
import time

from thefuzz import fuzz

try:
    import deepl
except ImportError:  # allow cache-only runs without the client installed
    deepl = None

# --- config -----------------------------------------------------------------

HERE = os.path.dirname(os.path.abspath(__file__))
CONCEPTS_PATH = os.path.join(HERE, "..", "concepts.json")
SUBJECTS_PATH = os.path.join(HERE, "subjects.csv")
OUTPUT_PATH = os.path.join(HERE, "translate_fuzzy_match_res.csv")
CACHE_PATH = os.path.join(HERE, "translations_cache.json")

# Languages the enriched vocabulary should carry. "en" is the source and is
# never translated; the rest are filled only where a concept lacks that label.
TARGET_LANGS = ["en", "fr", "de", "it", "es", "nl", "pt"]
SOURCE_LANG = "en"

# Map our short language codes to the target codes DeepL expects.
DEEPL_TARGET_CODES = {
    "fr": "FR",
    "de": "DE",
    "it": "IT",
    "es": "ES",
    "nl": "NL",
    "pt": "PT-PT",
}

# DeepL auth key from the environment (free keys end in ":fx"; the client
# auto-routes to the free endpoint). No key -> cache-only enrichment.
DEEPL_AUTH_KEY = os.environ.get("DEEPL_AUTH_KEY")

FUZZY_THRESHOLD = 80   # 0-100 (fuzz.ratio); placeholder to re-calibrate

# fuzz.ratio is relative to length, so on very short strings a single edit still
# scores high (e.g. oil/soil = 86). Subjects this short or shorter skip the
# fuzzy stage entirely; they can still match via URL or exact label.
MAX_SKIP_FUZZY_LEN = 4


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


def best_fuzzy_match(cons, key_value):
    """
    Return (concept, best_score, best_label, best_lang) for the concept whose
    highest label fuzz ratio is greatest across ALL languages, translated ones
    included. Returns (None, 0, None, None) if there are no labels or the key
    is too short to fuzzy-match reliably (<= MAX_SKIP_FUZZY_LEN chars).
    """
    key = format_string(key_value).lower()
    best = (None, 0, None, None)
    if not key or len(key) <= MAX_SKIP_FUZZY_LEN:
        return best
    for c in cons:
        for lang, labels in c["labels"].items():
            for lab in labels:
                ratio = fuzz.ratio(key, lab.lower())
                if ratio > best[1]:
                    best = (c, ratio, lab, lang)
    return best


# --- translation / enrichment ----------------------------------------------

def load_cache():
    if os.path.exists(CACHE_PATH):
        with open(CACHE_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_cache(cache):
    with open(CACHE_PATH, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)


def cache_key(text, lang):
    return f"{lang}\t{text}"


def enrich_vocab(cons):
    """
    Fill missing target-language labels by translating each concept's English
    prefLabel with DeepL. Existing (curated) labels are left untouched. Adds a
    private `_mt_langs` list to each enriched concept so matches can flag when
    the label that fired came from machine translation. Mutates `cons` in place.
    """
    cache = load_cache()

    # Collect the work: which (source_text, lang) translations are still needed.
    # Group by target language so each DeepL call is a single batched request.
    needed = {lang: [] for lang in DEEPL_TARGET_CODES}   # lang -> [source_text]
    for c in cons:
        en = c["labels"].get("en")
        source = en[0] if en else None
        if not source:
            continue  # nothing to translate from
        for lang in DEEPL_TARGET_CODES:
            if c["labels"].get(lang):
                continue  # keep the curated AGROVOC/ISO label; do not overwrite
            if cache_key(source, lang) in cache:
                continue  # already translated on a previous run
            needed[lang].append(source)

    # De-duplicate: many concepts share label text; translate each string once.
    to_translate = {lang: sorted(set(texts)) for lang, texts in needed.items() if texts}
    pending = sum(len(v) for v in to_translate.values())

    if pending:
        if DEEPL_AUTH_KEY and deepl is not None:
            translator = deepl.Translator(DEEPL_AUTH_KEY)
            for lang, texts in to_translate.items():
                print(f"  DeepL: translating {len(texts)} labels EN -> {lang} ...")
                results = translator.translate_text(
                    texts, source_lang="EN", target_lang=DEEPL_TARGET_CODES[lang]
                )
                # translate_text preserves order and returns a list for list input
                for src, res in zip(texts, results):
                    cache[cache_key(src, lang)] = res.text
            save_cache(cache)
            print(f"  Cached {pending} new translations to {os.path.basename(CACHE_PATH)}")
        else:
            print(f"  WARNING: {pending} labels need translation but no DEEPL_AUTH_KEY "
                  f"is set (and/or the deepl client is missing). Proceeding with the "
                  f"cache only; those language slots stay empty.")

    # Apply cached translations into the in-memory vocabulary.
    added = {lang: 0 for lang in DEEPL_TARGET_CODES}
    for c in cons:
        en = c["labels"].get("en")
        source = en[0] if en else None
        if not source:
            continue
        for lang in DEEPL_TARGET_CODES:
            if c["labels"].get(lang):
                continue
            translation = cache.get(cache_key(source, lang))
            if translation:
                c["labels"].setdefault(lang, []).append(translation)
                c.setdefault("_mt_langs", []).append(lang)
                added[lang] += 1

    total_added = sum(added.values())
    print(f"Enriched vocabulary with {total_added} translated labels: "
          + ", ".join(f"{lang}+{n}" for lang, n in added.items() if n))
    return cons


# --- main -------------------------------------------------------------------

def run():
    start_time = time.time()

    with open(CONCEPTS_PATH, "r", encoding="utf-8") as f:
        cons = json.load(f)
    print(f"Loaded {len(cons)} concepts")

    # Enrich the in-memory vocabulary (concepts.json on disk is left unchanged).
    enrich_vocab(cons)

    with open(SUBJECTS_PATH, newline="", encoding="utf-8") as f:
        subjects = list(csv.DictReader(f))
    print(f"Loaded {len(subjects)} subjects")

    results = []

    for sub in subjects:
        sub_id = clean_cell(sub.get("id"))
        sub_uri = clean_cell(sub.get("uri"))
        sub_label = clean_cell(sub.get("label"))

        row = {
            "method": "no_match",
            "subject_label": sub_label,
            "vocab_label": None,
            "matched_label": None,
            "matched_lang": None,
            "is_mt": None,
            "subject_id": sub_id,
            "subject_uri": sub_uri,
            "vocab_identifier": None,
            "fuzzy_score": None,
        }

        # 1. URL match
        if sub_uri is not None:
            matched = url_match(cons, sub_uri)
            if matched is not None:
                row["method"] = "url_match"
                row["vocab_identifier"] = matched["identifier"]
                row["vocab_label"] = primary_label(matched)
                results.append(row)
                continue

        # No label -> nothing more we can do
        if sub_label is None:
            results.append(row)
            continue

        # 2. Exact label match
        matched = exact_label_match(cons, sub_label)
        if matched is not None:
            row["method"] = "exact_match"
            row["vocab_identifier"] = matched["identifier"]
            row["vocab_label"] = primary_label(matched)
            results.append(row)
            continue

        # 3. Fuzzy match against the enriched vocabulary
        concept, score, flabel, flang = best_fuzzy_match(cons, sub_label)
        if concept is not None:
            # Record the best fuzzy candidate even when below threshold (for tuning)
            row["fuzzy_score"] = score
            row["matched_label"] = flabel
            row["matched_lang"] = flang
            row["is_mt"] = flang in concept.get("_mt_langs", [])
            if score >= FUZZY_THRESHOLD:
                row["method"] = "translate_fuzzy_match"
                row["vocab_identifier"] = concept["identifier"]
                row["vocab_label"] = primary_label(concept)
        results.append(row)

    # Write output: only translate_fuzzy matches (exclude url/exact/no_match).
    # The method column is dropped since every row is a translate_fuzzy_match.
    matched_rows = [r for r in results if r["method"] == "translate_fuzzy_match"]
    fieldnames = [
        "subject_label", "vocab_label", "matched_label", "matched_lang", "is_mt",
        "subject_id", "subject_uri", "vocab_identifier", "fuzzy_score",
    ]
    with open(OUTPUT_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f, fieldnames=fieldnames, quoting=csv.QUOTE_ALL, extrasaction="ignore"
        )
        writer.writeheader()
        writer.writerows(matched_rows)

    print(f"\nWrote {len(matched_rows)} translate_fuzzy_match matches "
          f"(of {len(results)} subjects) to {OUTPUT_PATH}")

    elapsed_min = (time.time() - start_time) / 60
    print(f"Total execution time: {elapsed_min:.2f} minutes")


if __name__ == "__main__":
    run()
