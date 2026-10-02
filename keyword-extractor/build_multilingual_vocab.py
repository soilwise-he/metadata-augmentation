"""
Build the multilingual extraction vocabulary.

Two problems with concepts.json as it stands, both of which hurt keyword
*extraction* from free text (they matter less for keyword-matcher, where a
harvested subject may literally be a measurement code):

  1. It contains sosa:Procedure concepts -- measurement-procedure identifiers
     such as "ExchAcid_ph0-kcl1m" or "CaCO3_acid-hcl-dc". These never occur in
     a title or abstract in any language, so they are pure noise here.
  2. Only ~13% of concepts carry a non-English label, so a German or French
     record has almost nothing to match against in its own language.

This script fixes both:

  - Procedures are identified from ../keyword-matcher/vocabs/SoilVoc.ttl (typed
    `a skos:Concept, sosa:Procedure`) and dropped from concepts.json.
  - Missing language slots are filled from the DeepL cache already built by
    ../keyword-matcher/translate-fuzzy-testing/translations_cache.json. No API
    call is made -- cache only. Curated AGROVOC/ISO labels are never
    overwritten; MT only fills languages a concept has no label for at all.

Every label carries its provenance in `label_sources` ("curated" vs
"mt-deepl") so downstream stages can trust the two differently: an exact-match
tier should probably require curated labels, while an embedding tier can use
everything. The machine translations are NOT reviewed -- context-free MT of
1-3 word technical labels misfires in predictable ways ("building stability"
-> "Stabilitaet schaffen"), so treat mt-deepl labels as recall aids, not truth.

Input  : concepts.json (this folder)
         ../keyword-matcher/vocabs/SoilVoc.ttl
         ../keyword-matcher/translate-fuzzy-testing/translations_cache.json
Output : concepts_multilingual.json (this folder)

Run: ../.venv/bin/python build_multilingual_vocab.py
"""

import json
import os
import time

from rdflib import Graph

# --- config -----------------------------------------------------------------

HERE = os.path.dirname(os.path.abspath(__file__))
CONCEPTS_PATH = os.path.join(HERE, "concepts.json")
TTL_PATH = os.path.join(HERE, "..", "keyword-matcher", "vocabs", "SoilVoc.ttl")
CACHE_PATH = os.path.join(
    HERE, "..", "keyword-matcher", "translate-fuzzy-testing", "translations_cache.json"
)
OUTPUT_PATH = os.path.join(HERE, "concepts_multilingual.json")

# Languages the vocabulary should carry. "en" is the MT source and is never
# translated; the rest are filled only where a concept has no label at all.
LANGS = ["en", "fr", "de", "it", "es", "nl", "pt"]

PROCEDURE_QUERY = """
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX sosa: <http://www.w3.org/ns/sosa/>

SELECT DISTINCT ?concept
WHERE {
    ?concept a skos:Concept, sosa:Procedure .
}
"""


def primary_label(concept):
    """First English label of a concept, or "" when it has none."""
    labels = concept.get("labels", {}).get("en", [])
    return labels[0] if labels else ""


def procedure_ids(ttl_path):
    """Identifiers of every concept typed sosa:Procedure in the TTL."""
    graph = Graph()
    graph.parse(ttl_path, format="ttl")
    return {str(row["concept"]) for row in graph.query(PROCEDURE_QUERY)}


def load_cache(cache_path):
    """DeepL cache as {lang: {source_label: translation}}.

    Cache keys are "<lang>\\t<source label>"; the source is always the English
    primary label.
    """
    with open(cache_path, encoding="utf-8") as f:
        raw = json.load(f)

    by_lang = {}
    for key, translation in raw.items():
        lang, _, source = key.partition("\t")
        if lang and source:
            by_lang.setdefault(lang, {})[source] = translation
    return by_lang


def main():
    start = time.time()

    with open(CONCEPTS_PATH, encoding="utf-8") as f:
        concepts = json.load(f)
    print(f"concepts.json: {len(concepts)} concepts")

    # --- 1. drop procedures -------------------------------------------------
    procedures = procedure_ids(TTL_PATH)
    print(f"SoilVoc.ttl: {len(procedures)} concepts typed sosa:Procedure")

    kept = [c for c in concepts if c["identifier"] not in procedures]
    dropped = len(concepts) - len(kept)
    print(f"dropped {dropped} procedure concepts -> {len(kept)} remaining")

    # Procedures present in the TTL but not in concepts.json (or vice versa)
    # would mean the two files have drifted apart -- worth knowing about.
    unmatched = procedures - {c["identifier"] for c in concepts}
    if unmatched:
        print(f"WARNING: {len(unmatched)} procedure URIs not found in concepts.json")

    # --- 2. fill missing languages from the MT cache ------------------------
    cache = load_cache(CACHE_PATH)
    print(f"translation cache: {sum(len(v) for v in cache.values())} entries, "
          f"languages {sorted(cache)}")

    filled = {lang: 0 for lang in LANGS}
    missed = {lang: 0 for lang in LANGS}
    out = []

    for concept in kept:
        labels = {lang: list(concept["labels"][lang])
                  for lang in concept["labels"] if concept["labels"][lang]}
        sources = {lang: "curated" for lang in labels}

        source_label = primary_label(concept)
        for lang in LANGS:
            if lang == "en" or labels.get(lang):
                continue  # never translate the source, never overwrite curated
            translation = cache.get(lang, {}).get(source_label)
            if translation:
                labels[lang] = [translation]
                sources[lang] = "mt-deepl"
                filled[lang] += 1
            else:
                missed[lang] += 1

        out.append({
            "identifier": concept["identifier"],
            "uris": concept.get("uris", []),
            "labels": {lang: labels[lang] for lang in LANGS if lang in labels},
            "label_sources": {lang: sources[lang] for lang in LANGS if lang in sources},
        })

    print("\nlanguage coverage (concepts with >=1 label):")
    print(f"  {'lang':<5} {'curated':>8} {'mt-deepl':>9} {'total':>7} {'no label':>9}")
    for lang in LANGS:
        curated = sum(1 for c in out if c["label_sources"].get(lang) == "curated")
        mt = sum(1 for c in out if c["label_sources"].get(lang) == "mt-deepl")
        print(f"  {lang:<5} {curated:>8} {mt:>9} {curated + mt:>7} {missed[lang]:>9}")

    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    print(f"\nwrote {OUTPUT_PATH} ({len(out)} concepts)")
    print(f"total time: {time.time() - start:.1f}s")


if __name__ == "__main__":
    main()
