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
  reproducible without DB access.
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

### 2. `keybert/` — KeyBERT (folder exists, empty)

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
- Records CSV snapshot not yet exported into this folder.
- How many keywords per record to keep (fixed top-k vs. score threshold) —
  decide after looking at score distributions.
