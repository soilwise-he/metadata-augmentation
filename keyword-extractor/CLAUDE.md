# Keyword extractor — experiments

Goal: for each harvested metadata record, propose SoilVoc keywords **from the
record's free text** (title, abstract, …). This differs from the sibling
`../keyword-matcher/`, which matches *already-harvested subject keywords* to
the vocabulary; here there is no keyword yet — the input is running text and
the task is extraction + vocabulary assignment in one.

Everything in this folder is **offline experimentation**: read records from a
CSV snapshot (or the DB read-only), write local CSVs, never write to the
database. A production script comes later, once an approach wins.

## Inputs

- **Records:** Postgres `metadata.records` — relevant columns `identifier,
  title, abstract` (see `../spatial-metadata-NER/README.md` for the table
  shape; `../NER augmentation/GliNER_augmenter.py` shows the canonical
  SELECT). Connection via `utils.database.dbInit()` using env vars
  `POSTGRES_HOST/PORT/DB/USER/PASSWORD`. For testing, prefer a CSV snapshot
  (`identifier,title,abstract`) checked into the experiment folder so runs are
  reproducible without DB access. `snapshot/records.csv` holds 27,674 records
  (`identifier,title,abstract`, no language column). It is **not English-only**
  — the first row is German, so any experiment run over the snapshot must say
  what it does with non-English text.
- **Vocabulary:** SoilVoc. Original TTL:
  `../keyword-matcher/vocabs/SoilVoc.ttl` (SKOS, `eusoilvoc:` namespace,
  ~1048 concepts, `skos:prefLabel` mostly English-only, `skos:exactMatch`
  links to AGROVOC/GEMET/INRAE/ISO 11074/SoilPhysics). Parsed form:
  `concepts.json` in this folder — a list of
  `{identifier, uris, labels: {lang: [..]}}`. It is built by
  `../keyword-matcher/get_thesaurus.py` (which also pulls multilingual labels
  from AgroVoc and ISO 11074). **Note:** this folder's `concepts.json` (Jun 16)
  differs from `../keyword-matcher/concepts.json` (Jul 12) — check freshness
  before relying on it. `../keyword-matcher/enriched-ce-testing/` additionally
  builds `enriched_concepts.json` with `skos:definition` text (~50% coverage),
  useful wherever concept *descriptions* help.
- **Vocabulary to actually use here:** `concepts_multilingual.json`, built by
  `build_multilingual_vocab.py` (cache-only, no network, ~0.3 s). It derives
  from `concepts.json` with two changes:
  - **Procedures dropped.** 249 concepts typed `a skos:Concept, sosa:Procedure`
    in the TTL are measurement-procedure identifiers (`ExchAcid_ph0-kcl1m`,
    `CaCO3_acid-hcl-dc`) that never occur in running text. **1048 → 799.**
    The RDF typing is the right filter — after the drop, zero code-like labels
    remain. Keep them in keyword-matcher, where a harvested subject may
    literally *be* such a code; they are noise only for extraction.
  - **All seven languages filled.** Missing slots come from the DeepL cache at
    `../keyword-matcher/translate-fuzzy-testing/translations_cache.json`.
    Coverage is now 799/799 for en/fr/de/it/es/nl/pt, with zero cache misses.
    Curated labels are never overwritten.

  Each concept carries `label_sources: {lang: "curated"|"mt-deepl"}` —
  provenance is per-language because MT fills whole empty slots. Curated
  German is 133 concepts; the other 666 are machine-translated and
  **unreviewed**. Treat `mt-deepl` labels as recall aids, not truth: an
  exact-match tier should require `curated`, an embedding tier may use both.
  Context-free MT of short technical labels misfires predictably
  (`building stability` → `Stabilität schaffen`, `aluminium exchangeable base`
  → `austauschbarer Sockel aus Aluminium`).

## Output (per record)

A ranked list of SoilVoc concepts: at minimum
`record_identifier, concept_identifier, concept_label, score, method`.
Keep scores and the method label in every row so approaches can be compared
on the same records. Eventual production target follows the pipeline
convention (`metadata.augments` + `metadata.augment_status`, or a dedicated
keyword table feeding the `mv_records` view), but no experiment writes there.

## The three approaches

One subfolder per approach (per-implementation subfolders where needed), each
self-contained: its own copy of the input snapshot, its own results CSV.

### 1. `embedding/` — bi-encoder retrieval (+ cross-encoder rerank)

Reuse the `../keyword-matcher/` CE stack, but note the task is now
**asymmetric**: a multi-sentence abstract vs. a 1–3 word concept label, unlike
the phrase↔phrase matching there.

- Bi-encoder: `sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2`
  is symmetric — fine for phrase↔phrase, questionable for document↔label.
  Either chunk the record text (title, abstract sentences) and retrieve per
  chunk, or try an asymmetric retrieval model; state the choice in the script.
- Cross-encoder rerank: `cross-encoder/mmarco-mMiniLMv2-L12-H384-v1` with
  `activation_fn=torch.nn.Sigmoid()` (scores 0–1). It is a query→passage
  model: feeding it *concept-as-query vs. record-text-as-passage* (or concept
  `enriched_text` from the definitions build) is closer to its training
  distribution than label-vs-label.
- Cache concept embeddings (`.npz` keyed by a hash of labels + model name),
  batch all CE pairs into one `predict()`, log every scored pair to a
  candidates CSV before thresholding — same pattern as keyword-matcher.

### 2. `keybert/` — KeyBERT

Exploration so far lives in `keybert/keybert_testing.ipynb` (notebook kernel is
`.venvipynb`, **not** `../.venv` — it has `keybert`/`sentence-transformers`/
`sklearn` but **no `thefuzz`**, so notebook cells must stay stdlib-only or the
package has to be installed there).

KeyBERT extracts keyword phrases from a document by embedding similarity.
Several implementations are worth separate experiments:

- **Free extraction + match:** vanilla KeyBERT over title+abstract (tune
  `keyphrase_ngram_range`, `use_mmr`/`use_maxsum` for diversity), then map
  the extracted phrases to SoilVoc — effectively generating input for a
  keyword-matcher-style cascade (URL/exact/fuzzy/CE).
- **Vocabulary-constrained:** pass SoilVoc labels as `candidates=` so KeyBERT
  scores only vocabulary terms against the document — extraction and
  assignment collapse into one step; no matching stage needed.
- **Seeded:** `seed_keywords=` to bias extraction toward the soil domain
  while still extracting freely.
- **Backbone variants:** default MiniLM vs. a domain/multilingual
  sentence-transformer; KeyLLM (KeyBERT's LLM mode) overlaps with approach 3.

Name each variant's folder/script and `method` label explicitly
(e.g. `keybert_free`, `keybert_vocab`, `keybert_seeded`).

#### Finding: `candidates=` is a lexical filter, not a semantic one

**`candidates=` and `vectorizer=CountVectorizer(vocabulary=…)` are the same
code path, and it is purely lexical.** KeyBERT builds a doc-term matrix over
the vocabulary and keeps only terms with a nonzero count in that document
(`df[index].nonzero()`), so a candidate must occur in the text *literally*
before the encoder ever scores it. Consequences:

- An English vocabulary against a German document returns `[]`. A multilingual
  backbone does **not** fix this — it changes only the scoring stage, and
  there is nothing left to score. (The notebook's note "need a multilingual
  embedding model" is only half the story.)
- The candidate list must be lowercased — the CountVectorizer lowercases the
  document. Keep a `lower→(identifier, en label, de label)` map to report both
  labels back.
- `concepts_multilingual.json` yields **854** German candidates from 799
  concepts: some concepts carry several German labels (`soil erosion` has
  *Erosion*, *Bodenerosion*, *Erosionserscheinungen*), each its own candidate
  pointing at the same concept.

#### Finding: German records (`keybert_vocab_de`)

The snapshot contains German records, so this is not a later refinement.
Established on the German test record (`ch.bafu.…-phosphor`, in the notebook):

Feeding the 854 German labels as `candidates=` lifts the record from 0 hits to
exactly **1** — `Bodenerosion` → soil erosion, 0.4379. That is the ceiling of
the approach, not a tuning problem. Four reasons the rest go missing (first
three fixable, the fourth is correct behaviour):

1. **The lexical pre-filter** discards candidates before scoring (above).
2. **German compounding** — the vocabulary term hides inside a longer word.
   `Phosphor`, `Landnutzung` and `Bodenauswaschung` are all in the vocabulary;
   the text writes `Phosphoreinträge`, `Landnutzungskategorie`, `Auswaschung`.
   Vocabulary-constrained KeyBERT is structurally weaker in German than in
   English for this reason alone.
3. **The label uses a different word than the document** — text `Drainage` vs.
   label `Entwässerung`; text `Abschwemmung` vs. label `Abfluss`; text `Wald`
   vs. label `Flächennutzung: Wälder`. MT gives one wording per concept; real
   writing uses the others. See the `altLabel` open item.
4. **Genuinely out of scope** — `Gletscher`, `Dauergrünland` and atmospheric
   deposition have no SoilVoc concept in any language. Not a bug.

#### Working shape: free extraction, then a cascade (`keybert_free_de`)

Let KeyBERT extract German phrases with no vocabulary (it does this well —
but pass German stop words, the default is `"english"`), then match the phrases
to labels yourself, most certain first, tagging each result with the tier that
produced it:

1. `de_exact` — casefolded label equals the phrase or one of its words.
2. `de_substring` — label inside a phrase word (`Phosphor` ⊂
   `phosphoreinträge`) or a phrase word inside a *single-word* label
   (`auswaschung` ⊂ `Bodenauswaschung`). Guard at ≥5 characters, or `Ton`
   matches half the dictionary. Matching a word inside a *multi-word* label
   lets function words through (`basierend` → `Index basierend auf Textur`).
3. `de_semantic` — nearest label by cosine, high bar only (0.90).

On the test record this returns 2 clean hits at `top_n=15, diversity=0.6`
(`Bodenerosion`, `Phosphor`) and 4 correct + 3 false positives at
`top_n=30, diversity=0.4` — the *extraction* stage is the limiter, not the
matching. Two false positives there came from the productive prefix `Gesamt-`,
which behaves like a stop word inside compounds.

**The embedding model cannot discriminate German compounds.** This is why the
semantic tier is last and gated. Nearest German labels to `bodenerosion`:

    0.8469  Bodentaxonomie      soil classification
    0.8425  Bodensedimentation  soil sedimentation
    0.8224  Bodenfestigkeit     soil strength
    0.8194  Bodenerosion        soil erosion   ← correct answer, ranked 6th

`paraphrase-multilingual-MiniLM-L12-v2` keys on the shared `Boden-` subword and
the whole band is 0.81–0.85. A pure-cosine matcher returns confident nonsense
(`atmosphärische deposition` → `Luftverhältnis`). The model is reliable when
words *look* different but *mean* the same (`Drainage`/`Entwässerung`), and
unreliable when they look alike but differ — the opposite of the English case,
where the encoder is the workhorse and string matching the fallback. For
German, cheap substring matching beats the encoder.

Also note: the `de_substring` length-ratio score is a tie-break within its
tier, **not** a ranking signal — at loose extraction settings it puts false
positives (0.96) above true positives (0.19). Sort by tier first. Using
`thefuzz.fuzz.partial_ratio` instead would fix this, but see the `.venvipynb`
caveat above.

#### Not yet done in `keybert/`

Everything so far is notebook-only on a single German record. Still missing:
no `.py` script, no results CSV, no candidates log, no run over
`snapshot/records.csv`, and no English-side re-run against the
procedure-filtered vocabulary (the notebook's English cells predate
`concepts_multilingual.json` and still use all 1048 concepts).

### 3. `llm/` — LLM extraction

Prompt an LLM with the record text and ask for SoilVoc concepts. Undecided:
provider/model, and how to fit ~1048 concepts into the prompt (full label
list, retrieval-shortlisted subset — i.e. approach 1 as a pre-filter — or
free extraction followed by matching). Constrain output to exact concept
identifiers/labels and validate against `concepts.json`; LLMs will otherwise
invent plausible near-miss labels. Log raw responses alongside parsed output.

## Shared conventions (inherit from keyword-matcher)

- Scripts use paths relative to their own location; run with
  `../../.venv/bin/python <folder>/<script>.py`.
- Shared env: `../.venv` at repo root (`sentence-transformers`, `torch`,
  `thefuzz`, `numpy`, `psycopg2`, …). **CPU-only box** — prefer small MiniLM
  models, bound candidate counts, batch inference.
- Helpers to replicate per script (keep them consistent):
  `all_labels()` / `primary_label()` over concepts; `clean_cell()` treating
  `""`/`NULL` as missing.
- Every run prints total execution time; every semantic stage writes a full
  candidates log (all scored pairs, pre-threshold) — that log, not the results
  CSV, is the material for threshold calibration.
- All thresholds are **placeholders until a gold set exists**. Scores from
  different models/input formats come from different distributions — never
  reuse a threshold across them.

## Open items

- **No gold set.** As in keyword-matcher: without hand-labelled records
  (record → correct SoilVoc concepts), the three approaches can only be
  compared qualitatively. A shared labelled sample of records should be the
  first artifact, and all experiments should run on that same sample.
- LLM provider/model not chosen.
- `concepts.json` staleness vs. the keyword-matcher copy (see Inputs).
- **Language distribution of the snapshot is unmeasured.** 27,674 records, no
  language column, German present. Run detection before deciding how much
  multilingual machinery is justified — it decides whether this is a German
  problem or a de/fr/it problem.
- **The 666 machine-translated German labels are unreviewed.** Cheapest QC is
  back-translation (de→en, compare to the original English label, flag the
  low-similarity rows) — that yields a review shortlist instead of eyeballing
  ~670 rows. Re-translating with the `skos:definition` context from
  `enriched_concepts.json` would also beat context-free DeepL.
- **One German word per concept is the main recall limit** (reason 3 above).
  AGROVOC/GEMET `skos:altLabel` synonyms are human-written and currently
  ignored by `get_thesaurus.py`, which also never queries GEMET or INRAE at
  all. Worth ~37 concepts upgraded from `mt-deepl` to `curated`, plus synonyms
  for the ones already curated.
- How many keywords per record to keep (fixed top-k vs. score threshold) —
  decide after looking at score distributions.
- Thresholds cannot be shared across languages. Multilingual encoders score
  same-language pairs higher than translation pairs, so a threshold calibrated
  on English will not transfer to German. Calibrate per language.
