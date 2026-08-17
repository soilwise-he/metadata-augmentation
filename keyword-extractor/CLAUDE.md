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
  (`identifier,title,abstract`, no language column). **19,611 have both a title
  and an abstract**; 4,040 have a title only, 4,023 have neither — so ~29% of
  the corpus carries no free text to extract from at all. Abstract length over
  the 19,611: p5 72, p25 336, median 1,299, p75 1,836, p95 2,755, p99 6,942
  chars. 3,726 records share their first 120 abstract characters with another
  record; the largest identical-abstract families are 364, 117 and 69.

  **Language distribution — measured** (lingua restricted to the seven SoilVoc
  languages, markup and URLs stripped, over the 19,611 records with an
  abstract):

      en  18,010  91.8%        unk  212  1.1%
      it     511   2.6%        es    84  0.4%
      fr     363   1.9%        nl    56  0.3%
      de     344   1.8%        pt    31  0.2%

  Non-English is **~7% (1,389 records)** and the largest non-English language is
  **Italian, not German**. Language is a property of a *text span*, not of a
  record: title and abstract disagree in 636 records (3.2%), and some abstracts
  are internally bilingual (a `[SPA] … [ENG] …` record; a 117-member family of
  German INSPIRE boilerplate with English schema definitions appended). Detect
  **per chunk** where the per-chunk language matters — on the bilingual records
  that splits them cleanly at confidence 1.00. 4.2% of abstracts score under
  0.90 confidence; that is the genuinely ambiguous set.

  Detector choice was benchmarked on 2,000 snapshot abstracts: `py3langid`
  (0.58 ms/doc), `lingua` restricted to 7 languages (1.03), `langdetect` (2.33)
  agree 98%+ on English-vs-not. Speed is irrelevant at this scale; short text is
  the differentiator, and `langdetect` wanders there (`'Data Management Plan'` →
  Indonesian). Restricting `lingua` to the seven languages makes that class of
  error impossible. A stopword-frequency heuristic was tried first and is **not
  adequate** — it mislabelled a large family of Italian records
  (`Temperatura del suolo … Corsa del …`) as Spanish and refused 7.4% of records
  as `unk` that are simply short English.
- **Test set:** `test_records_random.json` — 50 records drawn **uniformly at
  random** from the 19,611 with title+abstract. Rebuild with
  `make_random_sample.py [n] [seed]` (default seed 20260817; the pool is sorted
  by identifier first so the draw does not depend on CSV row order). Records
  carry `title`, `abstract`, char counts and lingua language labels; the language
  fields are observed *after* sampling, never selection criteria. **Inputs only
  — not a gold set.**

  Being representative is the point: it is the set to quote rates from, and it
  shows what a typical record looks like. Roughly a fifth of the draw is project
  administrivia or plainly off-topic (`Practice Abstracts 2`, `Final update of
  the Dissemination and Exploitation Plan`, a neglected-tropical-disease drug
  paper), which is itself a result — it sets how often the pipeline must
  correctly answer "SoilVoc does not apply here".

  **It cannot answer per-language questions.** The draw came out 48 en / 1 es /
  1 nl with no German, French or Italian, which is the expected consequence of
  random sampling at n=50 when non-English is ~7%. A 19-record curated set
  covering all seven languages, both length extremes, near-duplicate families and
  off-topic controls was built and then discarded in favour of random sampling;
  if a language or edge-case question comes back, that kind of set has to be
  rebuilt, and it must stay separate from the random one so rates are never
  quoted off a deliberately skewed sample.
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
- **This made chunk-level scoring the next experiment, not a refinement** — now
  run, see the next subsection. It works in English and *regresses* in German.
- Also note the generic-vs-specific inversion: *phosphorus* ranks 69 while
  *phosphorus total elements* ranks 1, and *land use* 287 while *land use
  class* is 7. The CE prefers the longer, more specific label — relevant to
  how results are deduped and reported.

#### Status: retrieve-and-rerank (bi-encoder → CE)

Prototyped as notebook cells, **still not saved into `ce_testing.ipynb`**; now
also implemented as `ce/retrieve_rerank.py` (see two subsections below, which
supersede these single-record numbers wherever they disagree). Shape: chunk the
record → bi-encoder top-k labels **per
chunk** → CE on the retrieved `(label, chunk)` pairs only → max-pool to one row
per concept. Same two models as above; the label side stays English throughout,
so German is still a cross-lingual test and `mt-deepl` labels are still unused.

**Chunking is not optional, it is a correctness fix.**
`paraphrase-multilingual-MiniLM-L12-v2` has `max_seq_length = 128` tokens and
mean pooling. The English test record is **658 tokens**, so a whole-document
embedding sees the first 128 — **19% of the abstract**, stopping mid-sentence —
and averages that. Anything named after that point cannot be retrieved at all.
Chunks of 200–350 chars land at 60–90 tokens, inside both models' limits (the
CE's `max_length=256` is a separate cap; chunking satisfies both).

**English record — the two-stage run works and is 7× faster.**
8 chunks → 160 pairs → 92 labels / 74 concepts = **8.6% of the vocabulary
reaches the CE**; 12 s total vs ~90 s for full scoring.

    ce     cos  chk  label
    0.9939 0.749  3  soil organic carbon        0.6906 0.703 3 soil organic carbon loss
    0.9769 0.616  3  soil organic matter content 0.5926 0.720 3 soil inorganic carbon
    0.9555 0.731  6  soil total carbon          0.5749 0.607 3 critical soil organic matter…
    0.8216 0.693  4  soil carbon density        0.5098 0.483 1 fertiliser use
    0.8028 0.627  3  soil organic component     0.4877 0.606 2 soil organic matter class

- **Retrieval cannot rank; the CE can.** Over the *same* 160 pairs, cosine spans
  0.400–0.749 while the CE spans 0.000–0.994. *soil functions* (cos 0.640)
  outranks *soil organic matter content* (cos 0.616) on cosine and scores
  **0.0026 vs 0.977** on the CE. The whole rejected tail is plausible soil
  vocabulary — *soil physical functions* 0.0019, *soil biodiversity loss*
  0.0005 — which is exactly why cosine cannot filter it.
- **Chunking adds recall, not just speed.** *fertiliser use* (0.51) comes only
  from chunk 1, at cosine 0.483; no document-level view surfaces it.
- **Max-pool per concept fixes the duplicate-label problem** listed below.
- **`chunk_text` caveats.** `lo` is the real knob — a chunk is flushed as soon
  as it passes `lo`, so `hi` almost never binds on prose. The sentence regex
  splits on abbreviations (`C.F. Gaertn` → new chunk), usually cosmetic after
  re-merging. And 3 of the 8 English chunks are pure statistics (`6.43 +/- 0.45
  G Kg(-1)`), consuming **~37% of the CE budget** for no new concept; a
  digit-ratio filter would recover it.

**Negative control — run, and the answer is clear.** Same pipeline, same
English labels:

    document                     max CE   ≥0.9  ≥0.5  ≥0.1
    EN soil (parkland C-13)      0.9939      3    10    24
    DE soil (phosphorus)         0.5546      0     1     8
    control: cardiology trial    0.0043      0     0     0
    control: satellite-imagery ML 0.0032     0     0     0

- **An absolute floor of ~0.05–0.1 is safe** — off-topic abstracts top out at
  0.004, two orders of magnitude below either soil record, with nothing in
  between. That floor answers "does SoilVoc apply to this record at all".
- **A shared cut across languages does not work.** 0.5 keeps 10 concepts in
  English and 1 in German. Prefer relative-per-record, e.g.
  `keep = ce >= max(0.10, 0.35 * top_score)` → 6 concepts EN, 2 DE.
- **No cut separates right from wrong *within* a record yet.** In English,
  *soil inorganic carbon* 0.593 and *critical soil organic matter content*
  0.575 sit above the correct *fertiliser use* 0.510. That needs the gold set,
  not a better constant.
- **Max-pooling biases toward long records**: a concept in an 8-chunk record
  gets 8 draws at a high score, a 2-chunk record 2. Fixed thresholds will
  therefore fire more often on long abstracts; relative rules are partly immune.

**German record — retrieve-and-rerank is a regression, and the loss is at the
retrieval stage.** Full scoring put *phosphorus total elements* at rank 1; the
two-stage run returns `LandUseClass 0.555 | SoilWaterContents 0.338 |
SoilWaterFlow 0.257 | SoilMoisture 0.223` and **no phosphorus, erosion,
leaching, runoff or drainage at all** — not down-ranked by the CE, never
shortlisted. Cause: the 530-char record yields **2 chunks**, and the cosine
ranks of the expected concepts in them are 50–642 (soil erosion 55, surface
runoff 50, drainage 67, leaching 141, phosphorus 642). At `top_k=20` none
survive. In English the shortlist is a harmless speedup; in German it deletes
the answers.

**Attempted fix — splitting parenthetical enumerations into their own passages
— improves retrieval and fails end-to-end.** Adding each item of
`(Bodenerosion, Auswaschung, Abschwemmung, Drainage, …)` as a passage takes the
record from 2 to 12 passages and moves cosine ranks to erosion **55→3**, runoff
**50→3**, land use **20→7**, drainage **67→19**. But the full pipeline is a net
negative:

    ce      psg  label                     ce      psg  label
    0.5546   1   land use class            0.2154   3   vegetation types  ← "Dauergrünland"
    0.3380   0   soil water contents       0.2084   7   erosion           ← correct, rank 7
    0.2571   0   soil water flow           0.1380   7   soil cracking     ← "Bodenerosion"
    0.2261   7   soil collapsing  ← noise  0.1136   4   sleet             ← "Wald"

- **A single German word is too little signal for either model.** Retrieval on
  bare items is near-random: `Wald` → *SOM* 0.74, *silt* 0.72; `Ackerland` →
  *boron* 0.51; `Auswaschung` → *exposition* 0.72; `Abschwemmung` → *flooding*
  **0.97** (confidently wrong). `Drainage` works only because it is a loanword,
  and `Bodenerosion` puts *soil erosion* 3rd–4th behind *soil deformation*.
- **The CE cannot score an 8-character "passage."** `mmarco-mMiniLMv2` is a
  query→passage relevance model; given `Drainage` as the passage it returns
  0.040, runoff 0.016, erosion 0.208 — so the retrieval win never converts.
- **Conclusion: fine granularity is right for retrieval and wrong for
  reranking.** The two stages want different passage sizes. Untested remedies:
  (a) give items context before the CE — `"Eintragspfade: Bodenerosion"` — or
  (b) retrieve on fine passages and rerank the resulting candidates against
  their *parent* chunk.
- **Beware measuring only the target's rank.** The enumeration split looked
  like a win when only the correct concepts' ranks were checked; it injected
  *soil collapsing*, *soil cracking*, *sleet*, *silt*, *boron*, *geology* at
  the same ranks. Always score what else arrives.

**Other German levers, measured but not yet wired in:**

- **German labels are complementary to English, not better** — `Bodenerosion`
  matches at cosine 1.000 and `Abfluss` at rank 1 for *Abschwemmung*, but
  leaching drops to 61 and phosphorus to 404. Union both shortlists for
  retrieval (recall only, so `mt-deepl` labels are acceptable *there*) and keep
  the rerank on the all-curated English labels.
- **A substring channel catches what dense retrieval structurally cannot.**
  German labels ≥5 chars found inside document tokens return 6 labels / 5
  concepts on this record, **all correct and all curated** (`Phosphor` ⊂
  *Gesamt-Phosphoreinträge*, `Bodenerosion`, `Erosion`, `Landnutzung`, `Boden`,
  `Klima`). Same tier as `keybert_free_de`'s `de_substring`, reused as a recall
  channel where its bad intra-tier scoring does not matter.
- **German needs a wider `top_k`** (40–50): short records make few chunks, so
  each chunk must carry more of the shortlist, and the CE cost stays trivial.

**Notebook code shape.** The cells keep `bi`/`ce`/label-embedding **caches
keyed by model name**, so a model swap costs one vocabulary encode and
switching back is free, plus a `PREFIXES` table because E5-family models need
`query:`/`passage:` markers. `ce_max_length` is a parameter and part of the CE
cache key.

**`nomic-embed-text-v2-moe` was attempted and the run is invalid — do not cite
it.** The notebook cell (exec 24) returns junk on the German record: cosine
collapsed into a 0.728–0.750 band and a top CE of 0.0858, below the applicability
floor. The cause is in the load report printed above the results, not the model:
`transformers` 5.x instantiated its own built-in `NomicBertModel`, which expects
a LLaMA-style `gate_proj`/`up_proj`/`down_proj` MLP, did not recognise nomic's
`mlp.experts.mlp.w1/w2` + `router` layout (logged `UNEXPECTED`), and **randomly
initialised the entire feed-forward stack of all 12 layers** (logged `MISSING`).
Roughly half the weights were noise; only embeddings and attention projections
survived. Two things to fix before retrying: pass `trust_remote_code=True` in
`get_bi`, and note that `PREFIXES` has no nomic entry, so the required
`search_document: `/`search_query: ` markers were never applied. **Clear
`_bi_cache` and `_emb_cache` when retrying** — `_emb_cache`'s key is
`(name, len(LABELS), hash(tuple(LABELS)))`, with no model config and no prefix in
it, so stale garbage embeddings are silently reused and the fix looks like it did
nothing. Verify by checking that the load report has zero `MISSING` keys and that
the cosine band is no longer ~0.02 wide.

So **still no alternative bi-encoder has been validly benchmarked.** Remaining
candidates are `LaBSE` (translation-trained, best at short cross-lingual phrases,
471M and slow), `paraphrase-multilingual-mpnet-base-v2`, and
`intfloat/multilingual-e5-small` (genuinely asymmetric, MiniLM-sized). Swap the
bi-encoder first: `mmarco-mMiniLMv2` is one of the few multilingual rerankers
small enough for this CPU box.

**Decision: German-specific exploration is stopped.** Not because the levers were
proven useless — nomic's test was invalid and `LaBSE` was never tried — but
because German is 344 of 19,611 records (1.8%) and English is 91.8%. The
justification is corpus composition, not model evidence; record it that way so
the door stays open. Chunk-size tuning *is* genuinely settled as useless there:
the German record is 530 chars and yields 2 chunks, and no `lo` fixes cosine
ranks of 50–642. If a non-English language is ever revisited it should be
**Italian**, which is 1.5× German.

Note the speed argument inverts for the tail: full CE scoring at ~1.5 min/record
is prohibitive for 17,186 English records (~430 h) but trivial for the 1,389
non-English ones (~35 h, one weekend). Two-stage is needed where it works and
unnecessary where it breaks, so routing English → two-stage and non-English →
full scoring is available at no new exploration cost. Untested, but it uses only
measurements already in hand.

#### Status: `ce/retrieve_rerank.py` — first script, first run at scale

The pipeline above as a script. Run:

    ../.venv/bin/python ce/retrieve_rerank.py [--input …] [--min-ce …] [--top-k …]

Defaults are the agreed configuration: `paraphrase-multilingual-MiniLM-L12-v2` →
`mmarco-mMiniLMv2-L12-H384-v1` (sigmoid, `max_length=256`), `lo=200`, `top_k=10`
labels per chunk, `--min-ce 0.1`, max-pool to one score per concept. Two choices
worth knowing, both flags: the document is **title + abstract** (`--text
abstract` switches it), and the label side is **English for every record**, so
non-English records are scored cross-lingually and `mt-deepl` labels stay unused.
**Language is read from the input JSON, never detected in the script.** It writes
the results CSV (`identifier, abstract, language, keywords`), a full pre-threshold
candidates CSV, and caches vocabulary embeddings in `ce/.cache/labels_<hash>.npz`
keyed by a hash of labels + model name.

**Run over `test_records_random.json` (50 records):**

    50 records -> 265 chunks -> 2,650 pairs (595 distinct labels reached the CE)
    114 s total, ~2.3 s/record

    threshold        concepts kept   per record   records with none
    ce >= 0.2                  159          3.2               14/50
    ce >= 0.1                  246          4.9                6/50

- **The CE rejects almost everything retrieval proposes.** Median CE over all
  2,650 pairs is **0.008**; only 9.3% clear 0.2 and 1.4% clear 0.9. Cosine over
  the same pairs spans 0.20–0.80 with median 0.52. Same conclusion as the
  notebook, now at scale: retrieval proposes, only the CE ranks.
- **0.1 is the better of the two, and 0.2 was doing two incompatible jobs.** As
  an *applicability* floor 0.2 is ~20× too high — genuinely off-topic records
  (`Practice Abstracts 2`, `Final update of the Dissemination and Exploitation
  Plan`) top out at **0.006**. As a *precision* cut it deletes correct answers:
  `soil quality` on a soil-quality dataset scored **0.195** and was dropped by
  0.005; `soil organic carbon` on a soil-carbon-farming record scored 0.130.
  Moving to 0.1 recovered 3 clearly-correct records against 2 clear false
  positives (`soil moisture deficit` on an anthropology paper about pastoralism;
  `groundwater depth` on till geochemistry). This is the same argument for a
  relative-per-record rule that the notebook reached from the other direction —
  the two jobs need two different rules, not one better constant.
- **A low floor is genuinely safe.** Off-topic records harvested into the corpus
  score 0.006–0.076, an order of magnitude below marginal true positives. That
  confirms the ~0.05–0.1 floor from the notebook's synthetic controls, now on
  real corpus records.
- **Short records fail at *scoring*, not thresholding.** `A review of existing
  soil monitoring systems` (166-char abstract, one chunk) tops out at **0.008**
  for `soil management`; `Landsat-based Spectral Indices for pan-EU` tops out at
  0.049. Lowering the floor cannot reach these. Records that make very few chunks
  are the open failure mode, and short abstracts are a large slice of the corpus
  (p25 = 336 chars).
- **Leakage is rare but real:** a neglected-tropical-disease drug paper returns
  `molybdenum`, an LC-HRMS serum-chemistry paper returns `effective CEC`.
- **Duplicate labels in the output are not a dedup bug.** Deduping by concept, as
  intended, still shows `soil health; soil health` because **10 English labels
  are shared by two concepts each** — 5 near-synonym pairs in SoilVoc
  (`SoilHealth`/`SoilQuality`, `SoilCohesion`/`SoilTexture`,
  `Microbes`/`Microorganisms`, `SoilMoisture`/`SoilWaterContents`,
  `SurfaceRunoff`/`Runoff`). Identical strings score identically, so both
  concepts survive at the same score. Deduping by *label* instead would silently
  drop a concept — decide which is wanted before reporting.

**English label shape (all 1,064 labels, 799 concepts, 1.33 labels/concept):**

    words  labels   share     cum          chars: min 3, median 18, max 43
        1     114   10.7%   10.7%          mean 2.49 words, median 2, max 7
        2     437   41.1%   51.8%
        3     402   37.8%   89.6%
        4     100    9.4%   99.0%
        5       9    0.8%   99.8%
        7       2    0.2%  100.0%

- **78.9% of the vocabulary is 2–3 words**, 89.6% is ≤3. Against a 200–400 char
  chunk that is a ~1:40 length ratio — the asymmetry noted at the top of this
  section, quantified.
- **The CE filters short labels out at ~3.5× their base rate.** Comparing the
  vocabulary against what the 50-record run surfaced:

        words   vocab  retrieved  ce>=0.1  top-1/record
            1   10.7%      7.5%     3.1%       10.0%
            2   41.1%     36.3%    34.3%       44.0%
            3   37.8%     46.1%    51.9%       42.0%
            4    9.4%      8.4%    10.1%        4.0%
        mean     2.49      2.61     2.71        2.40

  This is the generic-vs-specific inversion (*phosphorus* 69 vs *phosphorus total
  elements* 1) as a distribution. **But the bias lives in the tail, not the top**:
  the top-1 concept per record averages 2.40 words, *below* the vocabulary mean,
  and 1-word labels recover to their base rate there. Practically, lowering the
  floor mostly adds 3-word specifics — which is where the output redundancy comes
  from (`soil erosion; soil erosion category; soil erosion area affected; soil
  erosion degree` on one record).
- **23 labels are ≤4 characters** (`BNF, SOC, SOM, clay, crop, fog, hail, ion,
  ions, iron, mist, peat, rain, road, rock, root, salt, sand, silt, snow, soil,
  tree, zinc`) and 8 contain an all-caps token. Acronyms carry almost no signal
  for a sentence encoder — `effective CEC` is exactly the label that leaked onto
  the chemistry paper.
- **27 labels are element symbols in parentheses** (`Al (symbol)`, `Ca (symbol)`,
  …). Same category as the 249 dropped `sosa:Procedure` concepts: identifiers,
  not running-text terms. They still reach the CE. Dropping or rewriting them is
  a cheap precision gain.

Still missing / known issues:

- **Retrieval recall is still the binding constraint and is still unmeasured.**
  Whatever stage 1 drops, the CE never sees. The 50-record run does not measure
  this — it only ever saw the shortlist. The decisive cheap experiment is to run
  **both** paths on the same records (full CE over all 1,064 labels vs. two-stage)
  and report how many of full-scoring's top-N survive the shortlist. On a 20–50
  record slice that is under two hours of CPU, and it yields a defensible
  `top_k` — though only for English, since the random set has no other language.
- **Thresholds are not comparable across configurations.** Same model, but
  full-document, chunk and bare-term passages give three different score
  distributions (e.g. *soil organic matter content*: 0.879 full-doc, 0.977
  chunked). Compare ranks; calibrate per language *and* per passage size.
- **No gold set**, so "correct" above is my reading of the records, not labelled
  truth — including every precision/recall claim in the 50-record run. Both test
  sets are inputs only. This remains the top blocker for any claim that one
  approach beats another.
- **Short/few-chunk records are an open failure mode** (see the 0.008 example).
- The retrieve-and-rerank notebook cells are still not saved into
  `ce_testing.ipynb`, and there is still no run over the full snapshot.
- Resolved since the last revision: no `.py` script (now `ce/retrieve_rerank.py`),
  no results CSV, no candidates log, no test set (now two), and the snapshot's
  unmeasured language distribution.

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
Corpus volume for costing: 29.1M characters of title+abstract over the 19,611
records that have an abstract, of which only the non-English share would need
translating — now measured at **1.27M characters (4.4%)** across 1,389 records.
Cost is therefore not the obstacle; the egress policy is.

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
- ~~Language distribution of the snapshot is unmeasured.~~ **Measured — see
  Inputs.** English 91.8%; non-English ~7% of records (1,389) but only **4.4% of
  the text** (1.27M of 29.1M chars), because non-English records are shorter.
  Largest non-English language is **Italian** (2.6%), German is 1.8%.
  Consequence: most of the multilingual machinery described in this file was
  developed against 1.8% of the corpus. Any hosted-MT bill is small (~1.3M
  chars); the constraint on MT is the egress policy below, not cost.
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
