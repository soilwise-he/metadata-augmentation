# Keyword matcher — experiments

Production matcher is `match.py` (URL match → fuzzy label match, writes to the
`metadata.keyword_match` table). The folders below are **offline testing
experiments**. The first enriches the vocabulary with machine translation and
keeps a plain fuzzy match; the other three add a cross-encoder re-ranking stage
on top of that idea (the last also feeds the CE concept *definitions*). None
touch the database; all read/write local CSVs only.

## Task

Match harvested subject keywords to a controlled vocabulary
(~1048 concepts, labels in en/de/nl/it/fr/es). Each experiment runs the same
cascade — cheap exact steps first, then a semantic stage — and labels every
match with the method that produced it.

## Shared conventions (keep the scripts consistent)

All four experiments share the input/helper/output conventions below. The
cross-encoder bullet applies only to the three CE experiments; `translate-fuzzy-
testing/` has no semantic model (see its own section).

- **Input vocab:** `../concepts.json` — except `enriched-ce-testing/`, which
  builds its own vocabulary from a TTL (see its section). Each concept has
  `identifier`, `uris`, and `labels` keyed by language. Helpers `all_labels()`
  (flatten across languages) and `primary_label()` (English first, else first
  available) exist in every script.
- **Input subjects:** `subjects.csv` (`id, uri, label, ...`), copied into each
  folder so the folders are self-contained. `clean_cell()` treats `""` and
  `NULL` as missing; `format_string()` strips numeric label prefixes
  (e.g. `106022 mikrobiologie`).
- **Cascade order (all):** 1) URL match, 2) exact label match (case-insensitive,
  any language), 3) semantic stage. First hit wins; exact steps use no model.
- **Cross-encoder:** `cross-encoder/mmarco-mMiniLMv2-L12-H384-v1` (multilingual,
  MS MARCO). Loaded with `activation_fn=torch.nn.Sigmoid()` so scores are 0–1.
  `CE_THRESHOLD = 0.60` is the accept cutoff — a **placeholder to recalibrate on
  a labelled set**, not a validated value. The score is a sigmoid of a relevance
  logit, so a threshold from one model does not transfer to another — nor from
  one *input format* to another (see `enriched-ce-testing/`).
- **Output:** semantic-stage matches only — `url_match`, `exact_match`, and
  `no_match` rows are all excluded, and the `method` column is dropped (every
  row is the script's own CE method). The scripts share a column order,
  differing only in the retrieval-score column:
  `subject_label, vocab_label, subject_id, subject_uri, vocab_identifier,
  <fuzzy_score|cosine_score>, ce_score`.
- **Candidate logs:** with `LOG_CANDIDATES = True` (default), each script also
  writes every `(subject, candidate)` pair fed to the cross-encoder — before
  best-per-subject selection and thresholding — to
  `fuzzy_candidates_log.csv` / `embedding_candidates_log.csv` /
  `enriched_candidates_log.csv`
  (`subject_id, subject_label, candidate_vocab_identifier,
  candidate_vocab_label, matched_label, <fuzzy_score|cosine_score>, ce_score`).
  This is the raw material for threshold tuning; sub-threshold scores live
  here, not in the results CSV.
- **Batching:** the semantic stage is deferred — collect all `(subject,
  candidate)` pairs, run one batched `predict()`, then pick best-per-subject.
- **Timing:** each `run()` prints total execution time in minutes.

## `translate-fuzzy-testing/` — translate then fuzzy (no model)

Rather than a semantic model, **widen the vocabulary**. Most concepts are
English-only (AGROVOC/ISO give other-language labels for only ~13%), so this
experiment machine-translates each concept's English prefLabel into the target
languages to fill the *missing* slots, then runs a plain fuzzy cascade — no
cross-encoder. Method label: `translate_fuzzy_match`.

- **Enrichment:** DeepL API translates the English prefLabel into
  `TARGET_LANGS = [en, fr, de, it, es, nl, pt]`. `en` is the source (never
  translated); a language is filled **only when the concept has no label for it**,
  so curated AGROVOC/ISO labels are never overwritten. Enrichment is **in-memory
  only — `concepts.json` on disk is not rewritten.** DeepL target codes are mapped
  in `DEEPL_TARGET_CODES` (note `pt` → `PT-PT`). Enriched concepts get a private
  `_mt_langs` list so a match can flag whether the label that fired was machine-
  translated (`is_mt` column).
- **API key & caching:** set `DEEPL_AUTH_KEY` in the environment (free keys end
  in `:fx`; the client auto-routes). Translations are cached to
  `translations_cache.json` (keyed by `lang\tsource`) so re-runs don't re-spend
  quota. **Without a key the script still runs**, using only what's cached and
  warning about the rest — the fuzzy cascade then works on existing labels alone.
  Each target language is translated in one batched `translate_text()` call over
  the de-duplicated source strings.
- **Cascade:** 1) URL match, 2) exact label match, 3) fuzzy match over the
  enriched labels (all languages). Best concept by `fuzz.ratio`; if the best
  score ≥ `FUZZY_THRESHOLD = 80`, apply it. No CE stage. The 80 is a placeholder
  to recalibrate on a labelled set.
- **Short-subject guard:** subjects of ≤ 4 characters (`MAX_SKIP_FUZZY_LEN`)
  skip the fuzzy stage entirely — `fuzz.ratio` is length-relative, so one edit
  on a short string still scores high (`oil`/`soil` = 86). URL and exact match
  still apply to them. This guard exists only in this experiment, not in the
  CE scripts.
- **Output:** `translate_fuzzy_match_res.csv`, matches only, `method` column
  dropped. Columns: `subject_label, vocab_label, matched_label, matched_lang,
  is_mt, subject_id, subject_uri, vocab_identifier, fuzzy_score`. `matched_label`
  / `matched_lang` show which (possibly translated) label produced the hit.
- Run: `../../.venv/bin/python translate-fuzzy-testing/translate_fuzzy_match.py`.
- Needs the `deepl` package (installed in `../../.venv`).

## `fuzzy-ce-testing/` — fuzzy gate + CE rerank (CE baseline)

Candidates come from `thefuzz` (`fuzz.ratio ≥ FUZZY_THRESHOLD = 60`, lowered
from the original 70 to widen the gate), then the cross-encoder reranks them.
Method label: `fuzzy_cross_encoder`.

- Run: `../../.venv/bin/python fuzzy-ce-testing/fuzzy_ce_match.py`
  (from `keyword-matcher/`; the script uses paths relative to its own location).
- Output: `fuzzy-ce-testing/fuzzy_ce_match_res.csv`.
- **Known limitation:** `fuzz.ratio` is lexical (character edit distance), so it
  only surfaces spelling-similar candidates and misses semantic matches like
  `maize`↔`corn`. On the full 21k set at the original threshold of 70 it fed
  the CE only ~7360 pairs; ~87% of no-match subjects never produced a candidate
  at all. This is the motivation for the embedding experiment.

## `embedding-ce-testing/` — retrieve & rerank

Replaces the fuzzy gate with **bi-encoder embedding retrieval** (candidates by
meaning, not spelling), then CE rerank. Method label: `embedding_cross_encoder`.

- Bi-encoder: `sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2`
  (384-dim, multilingual, symmetric — suits phrase↔phrase).
- Retrieval: embed all concept labels once → cached to
  `concept_embeddings.npz` (invalidated by a hash of labels + bi-encoder name).
  Per subject, exact cosine top-k via `util.semantic_search` (`TOP_K = 15`),
  drop candidates below `COSINE_FLOOR = 0.40`, dedupe to best label per concept.
- Run: `../../.venv/bin/python embedding-ce-testing/embedding_ce_match.py`.
- Output: `embedding-ce-testing/embedding_ce_match_res.csv` (`cosine_score`
  column instead of `fuzzy_score`).
- **Trade-off:** far higher recall than the fuzzy gate, but retrieval surfaces
  candidates for most subjects, so the CE workload grows to ~(subjects × k).
  `COSINE_FLOOR` and `TOP_K` bound it; matters because the box is CPU-only.

## `enriched-ce-testing/` — definitions as the CE passage

Same retrieval cascade as `embedding-ce-testing/`, but the cross-encoder scores
the subject against a **concept passage** (`"{primary_en_label}: {definition}"`)
instead of a bare candidate label. Rationale: mmarco is a *query → passage*
relevance model, so keyword-vs-passage is closer to its training distribution
than the phrase-vs-phrase pairs the other two CE scripts feed it. Method label:
`enriched_cross_encoder`.

- **Own vocabulary build (phase 0).** This experiment does **not** read
  `concepts.json` (which has no definitions). It runs the `get_thesaurus.py`
  logic itself over `SoilVoc_skosmos.ttl` in the folder: SPARQL for concepts,
  en pref/altLabels, `skos:definition` and exact/close match URIs, then fetches
  multilingual labels from **AgroVoc** (remote SPARQL) and **ISO 11074** (local
  `../vocabs/ISO11074.ttl`). Definitions are blank nodes — the query traverses
  `skos:definition/rdf:value` — and `clean_definition()` strips the trailing
  `Source: …` citation tail.
- **Cache:** the built vocabulary is written to `enriched_concepts.json`
  (delete it to rebuild). Each concept gains `definition`, `enriched_text`
  (the CE passage; falls back to the bare *primary* label) and `has_definition`.
  **Coverage: 527/1048 concepts (~50%) have a definition** — the rest are
  scored label-only. Caution: the fallback is the primary (English-first)
  label, *not* the retrieved matched label that `embedding-ce-testing/` feeds
  its CE, so even the label-only CE scores here match that experiment's only
  where retrieval happened to hit the primary label (~1/3 of such pairs).
- **AgroVoc rate limiting:** the endpoint returns HTTP 429 under load, so
  requests are paced (`AGROVOC_DELAY_S`) and retried with exponential backoff
  (`AGROVOC_RETRIES`). If any URI still fails, the partial vocabulary is
  **deliberately not cached** — caching it would silently freeze missing labels
  in place; the run continues on what it has and re-running retries them.
- **Retrieval is unchanged** (bi-encoder over *labels*, `TOP_K`/`COSINE_FLOOR`
  as in `embedding-ce-testing/`): the bi-encoder is a symmetric phrase model, so
  embedding long definitions against 2-word keywords is the asymmetric task it is
  not built for. Only the CE input changes, which keeps the two candidate logs
  comparable per `(subject, concept)` pair.
- Run: `../../.venv/bin/python enriched-ce-testing/enriched_ce_match.py`.
- Output: `enriched_ce_match_res.csv`, plus a `has_definition` column in both
  the results and the candidate log.
- **Thresholds:** CE scores from label-only vs. label+definition passages come
  from *different distributions*, so one `CE_THRESHOLD` for both is unsound.
  Split the candidate log on `has_definition` and calibrate the two separately.
- **Known weak spot:** a non-English subject against an English passage scores
  poorly — e.g. `106022 mikrobiologie` retrieves the right German label
  (cosine 0.81) but the CE gives it ~0.015. Definitions are English-only, so
  the CE's cross-lingual ability is doing all the work there.

## Environment

- Python: `../../.venv` (has `sentence-transformers`, `torch`, `thefuzz`,
  `deepl`, `numpy`, `scikit-learn`). **CPU only** — no GPU, so prefer the small
  MiniLM models and keep the candidate count bounded.
- Models download from HuggingFace on first use (unauthenticated HF warning is
  harmless).

## Open items

- **No gold set yet.** All thresholds (`CE_THRESHOLD`, `COSINE_FLOOR`, `TOP_K`)
  are guesses. The intended next step is ~150–200 hand-labelled subjects to set
  them from a real precision/recall curve and to compare the experiments
  fairly. Until then, treat the outputs as exploratory, not validated.
