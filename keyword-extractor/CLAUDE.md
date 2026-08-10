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

### 1. `ce/` — cross-encoder (+ bi-encoder retrieval)

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

#### Status: `ce/ce_testing.ipynb`

Two cells, both run: one English record, one German record. Both skip
retrieval entirely and cross-encode **all 1064 English labels** (799 concepts)
against the document as `(label, document)` pairs — `max_length=256`, sigmoid,
~1.5 min/record on CPU. **The label side is English in both cells**, so the
German cell is a cross-lingual test and the unreviewed `mt-deepl` labels are
not involved anywhere in `ce/`.

**English record** (parkland C-13, same text as `keybert_testing.ipynb`):

    0.8791  soil organic matter content   0.4254  soil organic component
    0.8339  soil organic matter contents  0.2185  critical soil organic matter…
    0.7613  soil organic matter           0.1831  soil inorganic carbon
    0.7025  soil organic carbon           0.1815  soil organic matter class
    0.4383  soil organic components       0.1516  critical soil organic matter…

- **The CE reaches concepts no lexical method can.** *soil organic carbon*
  (0.70) is never a literal span in the text; vocabulary-constrained KeyBERT
  cannot return it at all. That is the structural advantage over approach 2.
- **The CE discriminates where cosine does not.** In a separate run with
  bi-encoder retrieval in front (chunked title + abstract sentences), all 40
  retrieved candidates sat in a 0.55–0.74 cosine band — including junk like
  *soil biological degradation* at 0.63 — while the CE spread the same set over
  0.07–0.997. Retrieval proposes; only the CE ranks.
- **Scores are text-sensitive, so they are not comparable across runs.** An
  earlier run on a hand-shortened version of this abstract (232 tokens, no
  truncation) put the same top four at 0.97/0.95/0.95/0.93. The cell now uses
  the full Title-Cased original, which exceeds `max_length=256` and is silently
  truncated. Same concepts, different numbers — do not mix them.

**German record** (`ch.bafu.…-phosphor`, same text as the KeyBERT notebook):

    0.8466  phosphorus total elements  ← de label is "Gesamtphosphor"
    0.6913  soil phosphorus loss           0.4143  soil particle movement
    0.5186  soil water loss                0.4046  land use class
    0.4520  soil P loss                    0.3834  soil organic carbon loss
    0.4472  soil water deficit             0.3286  soil water contents

- **Cross-lingual pairing works, and beats KeyBERT on the topic.** No
  translation, no lexical filter, no language detection — English labels
  against German text return the phosphorus concepts and *land use class*.
  Vocabulary-constrained KeyBERT returned exactly one hit on this record.
  Note the top hit corresponds to German *Gesamtphosphor* while the text writes
  *Gesamt-Phosphoreinträge*: the CE crossed both the language and the compound.
- **But whole-document scoring buries the concepts the text names outright.**
  Ranks of the expected answers over all 1064 labels:

        1  phosphorus total elements    183  soil erosion    ("Bodenerosion")
        7  land use class               403  surface runoff  ("Abschwemmung")
       69  phosphorus                   534  soil leaching   ("Auswaschung")
      287  land use                     796  soil drainage   ("Drainage")

  `Bodenerosion` is KeyBERT's one hit on this record and the CE puts it at 183.
  The pathway terms appear in a single parenthetical list, and scoring the
  whole document drowns them — the model answers "what is this document
  about?", which is *total phosphorus inputs*. The same effect is visible in
  English (*trees*, *roots*, *crops* miss the top 15 despite being literal
  words). So "the CE beats KeyBERT in German" is true for topic and false for
  recall of enumerated terms; the honest conclusion is that they fail in
  opposite directions.
- **This makes chunk-level scoring the next experiment, not a refinement.**
  Scoring per sentence and taking the max per concept would put
  "Bodenerosion, Auswaschung, Abschwemmung, Drainage" in a passage of its own.
- Also note the generic-vs-specific inversion: *phosphorus* ranks 69 while
  *phosphorus total elements* ranks 1, and *land use* 287 while *land use
  class* is 7. The CE prefers the longer, more specific label — relevant to
  how results are deduped and reported.

Still missing / known issues:

- **No negative control.** Every scored label is a soil term, so the numbers
  give a ranking but not a scale. Score the same labels against an unrelated
  abstract — the top score of the *wrong* document is the number that decides
  whether a threshold is possible at all.
- **Duplicate labels of one concept fill the top-k** (English ranks 1–2 and
  5–6 are two concepts across four rows; German 2 and 4 are both `SoilPLoss`).
  Dedupe to best-label-per-concept before reporting.
- **`max_length=256` silently truncates real records** — see the English cell
  above. In `snapshot/records.csv` **42% of abstracts exceed ~1000 characters
  (~256 tokens)** (p50 468, p90 2113, p99 5247). Raise the cap, chunk, or print
  token counts so truncation is visible.
- **Thresholds are not comparable across the two cells.** Same model, but
  same-language vs. cross-lingual pairs come from different distributions.
  Compare ranks, calibrate per language.
- **Full scoring does not scale**: 1064 pairs/record × 27,674 records ≈ 1.5
  min/record on CPU. A bi-encoder pre-filter is required for anything
  corpus-wide; the notebook's configuration is the quality ceiling, not a
  pipeline.
- No `.py` script, no results CSV, no candidates log, no run over the snapshot.

### 2. `keybert/` — KeyBERT

Exploration so far lives in `keybert/keybert_testing.ipynb` (notebook kernel is
`.venvipynb`, **not** `../.venv`). `.venvipynb` has `keybert 0.9.0`,
`sentence-transformers 5.6.0`, `torch 2.13.0`, `sklearn`, `yake`, `jellyfish`,
and `openai 2.53.0` (installed for approach 3). It has **no `thefuzz`** — but
`jellyfish` covers the same ground (`jaro_winkler_similarity`), so fuzzy
matching in notebook cells is available, just under a different API. No
`rdflib`/`SPARQLWrapper`/`psycopg2` there: no TTL parsing and no DB access from
the notebooks. Versions drift slightly from `../.venv` (ST 5.5.1, torch 2.12);
run both under `../.venv` if notebook and script numbers must match exactly.

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

#### Finding: KeyBERT can never emit English keywords from German text

Extraction is span selection, not generation: `extract_keywords` builds its
candidate pool with a `CountVectorizer` over the *document itself* and the
encoder only scores those n-grams. So a German document yields German phrases,
whatever the backbone. `seed_keywords=` does not change this either — it shifts
the document embedding the candidates are scored against, not the pool.

Consequences for the language problem: the only ways to get English concepts
out of German text are (a) extract German then map (the `keybert_free_de`
cascade above), (b) translate the document first, or (c) use a model that is
not span-bound — KeyLLM (approach 3) or the cross-encoder (approach 1, where
the cross-lingual cell now shows this working without any translation).

Translating the **document** dissolves both German failure modes (compounding
*and* one-wording-per-concept); translating the **vocabulary** fixes neither.
That makes document translation the stronger MT direction if MT is used at all.
Corpus volume for costing: 29.5M characters of title+abstract over 27,674
records (~7.4M tokens), of which only the non-English share would need
translating — and that share is still unmeasured (see open items).

#### Not yet done in `keybert/`

Everything so far is notebook-only, on one German record and one English
record. Still missing: no `.py` script, no results CSV, no candidates log, no
run over `snapshot/records.csv`, and no English-side re-run against the
procedure-filtered vocabulary — the notebook's English cells still load
`../concepts.json` (1303 en labels, procedures included) rather than
`concepts_multilingual.json` (1064 en labels, 799 concepts).

### 3. LLM extraction — KeyLLM, in `keybert/keyllm_testing.ipynb`

Prompt an LLM with the record text and ask for SoilVoc concepts. Being done
with **KeyLLM** (KeyBERT's LLM mode), so the notebook lives beside the KeyBERT
one rather than in an `llm/` folder.

**Provider: OpenRouter**, via the OpenAI SDK — `keybert.llm.OpenAI` takes a
client you construct, so `base_url="https://openrouter.ai/api/v1"` is the only
provider-specific line, and model IDs ending `:free` cost nothing. One key
gives Llama/Qwen/DeepSeek/Gemma/Mistral behind one string, which turns the
model comparison into a one-line change. Free limits: **20 RPM**, and **~50
requests/day** until a one-time $10 credit purchase raises it to 1,000/day
permanently. 50/day is one gold-set pass with no room to iterate on the prompt.

Caveat carried over from the free tiers generally: most `:free` variants
require enabling the OpenRouter setting that permits providers **which may
train on inputs**. Same question as the MT egress one — worth a single policy
answer from ISRIC rather than re-deciding per tool. If the answer is no, every
free API option dies at once and the fallbacks are local models
(`keybert.llm.TextGeneration`) or the cross-lingual CE, which needs no LLM.

#### What KeyLLM actually does (checked against the installed source)

`KeyLLM.extract_keywords(docs, check_vocab=False, candidate_keywords=None,
threshold=None, embeddings=None)`.

- **`candidate_keywords=` + the `[CANDIDATES]` prompt tag is the pattern to
  use here.** The backend does a plain `prompt.replace("[DOCUMENT]", doc)` and
  `.replace("[CANDIDATES]", ", ".join(candidates))`. Feed a ~30-concept
  shortlist from approach 1 and ask the model to *select*, not invent. That is
  the "retrieval-shortlisted subset" option, and it drops the prompt from
  ~5,000 tokens (whole vocabulary) to ~200. For reference, the full English
  label list **does** fit in a prompt: 1054 unique labels = 20,213 chars ≈
  5,000 tokens.
- **Do not use `check_vocab=True`.** It is a post-hoc, case-sensitive
  `if keyword in document` filter applied *after* the API call. It saves no
  tokens, drops correct answers on case alone, and reintroduces exactly the
  lexical trap that limits `candidates=` in approach 2 — it would delete
  *soil organic carbon*, the best answer on the English test record.
- **`threshold=` + `embeddings=` clustering is dangerous on this corpus.** It
  runs `util.community_detection(...)`, calls the LLM only for the first
  document of each cluster, and copies those keywords to every member. The
  snapshot is full of template families — records 1 and 2 are the same MODIFFUS
  boilerplate with nitrogen swapped for phosphorus — which at `threshold=.75`
  would cluster and lose the one distinction that matters. Use ≥0.90 if at all,
  and inspect the clusters.
- **The parser is `response.split(",")`.** No scores, no JSON (passing
  `response_format` through `generator_kwargs` still gets comma-shredded), no
  raw-response log. KeyLLM therefore **cannot produce the output convention**
  above (`score`, `method` per row).
- Wiring gotchas: pass `chat=True` (the default is `gpt-3.5-turbo-instruct` on
  the dead completions endpoint), plus `delay_in_seconds=3` and
  `exponential_backoff=True` for the 20 RPM cap.

**Plan:** use KeyLLM for the quick qualitative read — does LLM selection over a
CE shortlist beat CE reranking alone on a few records? Then write the real
experiment as a direct client loop returning `{concept_identifier,
confidence}` under a JSON schema, validated against
`concepts_multilingual.json` (LLMs invent plausible near-miss labels; count the
invalid rate, it is itself a result). Log raw responses alongside parsed output.

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
- **API keys come from the environment** (`OPENROUTER_API_KEY`, …), never
  literals in a notebook cell or script — the notebooks are in the repo. Same
  convention as the `POSTGRES_*` variables.

## Open items

- **No gold set.** As in keyword-matcher: without hand-labelled records
  (record → correct SoilVoc concepts), the three approaches can only be
  compared qualitatively. A shared labelled sample of records should be the
  first artifact, and all experiments should run on that same sample.
- LLM provider chosen (OpenRouter); **model not chosen** — that comparison is
  the point of the OpenRouter setup.
- **Whether record text may be sent to services that train on it** is
  unanswered, and it gates every free LLM tier and every hosted MT option.
  Needs one policy answer, not a per-tool decision.
- `concepts.json` staleness vs. the keyword-matcher copy (see Inputs).
- **Language distribution of the snapshot is unmeasured.** 27,674 records, no
  language column, German present. Run detection before deciding how much
  multilingual machinery is justified — it decides whether this is a German
  problem or a de/fr/it problem, and it sizes any MT bill (29.5M chars total,
  of which only the non-English share would be translated).
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
