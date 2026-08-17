"""Draw a uniformly random test sample of (identifier, title, abstract) from the snapshot.

No stratification, no hand-picking: every record with a non-empty title AND abstract has
an equal chance. The draw is reproducible from SEED and the pool is sorted by identifier
first, so the result does not depend on CSV row order.

    ../.venv/bin/python make_random_sample.py [n] [seed]

Language labels are attached as *observed properties* of the draw, never as selection
criteria. They need `lingua-language-detector`; without it the field is omitted.
"""
import csv, json, random, re, sys, time
from collections import Counter
from pathlib import Path

HERE = Path(__file__).resolve().parent
SNAPSHOT = HERE / "snapshot" / "records.csv"
OUT = HERE / "test_records_random.json"

N = int(sys.argv[1]) if len(sys.argv) > 1 else 50
SEED = int(sys.argv[2]) if len(sys.argv) > 2 else 20260817

csv.field_size_limit(10**8)
t0 = time.time()


def clean_cell(v):
    """'' and 'NULL' are both missing."""
    return "" if v is None or v.strip().upper() in ("", "NULL") else v.strip()


# --- optional language labelling -------------------------------------------
_MARKUP = re.compile(r"<[^>]+>")
_URLS = re.compile(r"https?://\S+|www\.\S+|\b10\.\d{4,}/\S+")


def strip_noise(t):
    return re.sub(r"\s+", " ", _URLS.sub(" ", _MARKUP.sub(" ", t))).strip()


try:
    from lingua import LanguageDetectorBuilder, Language

    _SEVEN = [Language.ENGLISH, Language.GERMAN, Language.FRENCH, Language.ITALIAN,
              Language.SPANISH, Language.DUTCH, Language.PORTUGUESE]
    _det = LanguageDetectorBuilder.from_languages(*_SEVEN).build()

    def detect_lang(text, min_chars=25):
        t = strip_noise(text)
        if len(t) < min_chars:
            return "unk", 0.0
        vals = _det.compute_language_confidence_values(t)
        return ((vals[0].language.iso_code_639_1.name.lower(), round(vals[0].value, 3))
                if vals else ("unk", 0.0))
except ImportError:
    detect_lang = None
    print("note: lingua not installed — language fields omitted")

# --- draw -------------------------------------------------------------------
pool = []
with open(SNAPSHOT, encoding="utf-8") as f:
    for row in csv.DictReader(f):
        title, abstract = clean_cell(row["title"]), clean_cell(row["abstract"])
        if title and abstract:
            pool.append({"identifier": row["identifier"], "title": title, "abstract": abstract})

pool.sort(key=lambda r: r["identifier"])
sample = random.Random(SEED).sample(pool, N)
print(f"pool {len(pool)} records with title+abstract -> sampled {len(sample)} (seed {SEED})")

for r in sample:
    r["n_chars_title"] = len(r["title"])
    r["n_chars_abstract"] = len(r["abstract"])
    if detect_lang:
        lg, conf = detect_lang(r["abstract"])
        r["lang_abstract"], r["lang_abstract_confidence"] = lg, conf
        r["lang_title"] = detect_lang(r["title"])[0]

doc = {
    "description": (
        "Uniformly random test sample of records with both a title and an abstract, drawn from "
        "snapshot/records.csv. Unstratified and unfiltered beyond the title+abstract requirement, "
        "so the composition reflects the corpus. Not a gold set: inputs only, no correct-concept "
        "labels."),
    "source": "snapshot/records.csv",
    "built_by": f"make_random_sample.py {N} {SEED}",
    "built": time.strftime("%Y-%m-%d"),
    "sampling": {
        "method": "random.Random(seed).sample over the eligible pool, no stratification",
        "seed": SEED,
        "pool_size": len(pool),
        "pool_definition": "non-empty title AND non-empty abstract ('' and 'NULL' both count as missing)",
        "pool_sorted_by": "identifier, so the draw is independent of CSV row order",
        "excluded_from_pool": "4040 records with a title but no abstract; 4023 with neither",
    },
    "caveats": [
        "A random draw of this size cannot support per-language conclusions: non-English is ~8% of "
        "the corpus, so the expected non-English count here is a handful and German/Dutch/Portuguese "
        "may be absent entirely. Use the curated set for language-specific questions.",
        "Text is verbatim from the snapshot, including markup and bilingual duplication.",
        "Language fields describe the abstract as a whole; records can be internally bilingual, so "
        "detect per chunk when the per-chunk language matters.",
    ],
    "n_records": len(sample),
    "records": sample,
}

with open(OUT, "w", encoding="utf-8") as f:
    json.dump(doc, f, ensure_ascii=False, indent=2)
    f.write("\n")

# --- describe what the draw happened to contain -----------------------------
if detect_lang:
    print("\nlanguage of abstract (observed, not selected):")
    for lg, n in Counter(r["lang_abstract"] for r in sample).most_common():
        print(f"  {lg:5s} {n:3d}  {n/len(sample):5.1%}")
    mism = sum(1 for r in sample if r["lang_title"] != r["lang_abstract"]
               and "unk" not in (r["lang_title"], r["lang_abstract"]))
    print(f"  title/abstract language mismatch: {mism}")

lens = sorted(r["n_chars_abstract"] for r in sample)
print("\nabstract length (chars):")
print(f"  min {lens[0]}  p25 {lens[len(lens)//4]}  median {lens[len(lens)//2]}  "
      f"p75 {lens[3*len(lens)//4]}  max {lens[-1]}")
print(f"\nwrote {len(sample)} records -> {OUT.relative_to(HERE)}   [{time.time()-t0:.1f}s]")
