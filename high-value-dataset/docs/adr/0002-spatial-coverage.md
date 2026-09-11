# 0002 — Spatial Coverage: the multi-country-EU classifier

The classifier fuses three independent signals — a bounding-box verdict
(:mod:`bbox_classifier`), the free-text ``spatial_desc`` column, and EU-related
terms extracted by :mod:`mcf_parser` — into one boolean
``is_multi_country_eu``. This document records the *whys* behind the fusion.
The *mechanism* (the reliability ladder, the provenance bands, and the
agreement flag) lives in the `spatial_coverage.py` module docstring;
a visual of the bbox side lives in
[`bbox-classifier-flowchart.md`](../bbox-classifier-flowchart.md); the
structural evidence in `spatial_coverage_analyses.ipynb`.

## The shared EU definition

Both this classifier and `bbox_classifier` use
`eu_terms_and_countries.DEFAULT_EU_COUNTRIES` — the **continental**-Europe set
(sovereign European states), **not** European Union, so Norway, Moldova,
the U.K., Switzerland, etc. are European on both signals. Country-name forms
are normalized via two alias maps, so a Norway-only record is treated
identically by its bbox and its text.

## The central thesis: reliability = f(provenance band, specificity)

A term's **reliability** — how decisive it is — is a function of two axes:

- **provenance band** — *where* the term was extracted (how deliberate the
  source is);
- **specificity** — *what* the term denotes: a `concrete` multi-country term >
  a `country` name > a `broad` term.

The same word's reliability is not absolute: a `country` in `place_keyword`
reaches S2 (sticky-against), the same `country` in `abstract` is ignored. Each
band has a **trust rule** — the function `f(band, specificity) → tier` — and the
four **reliability tiers** (S0–S3) are the output of that mapping. The pooled
tier is the **highest** across bands (and `spatial_desc`); the first matching
resolution rule fires (full table and steps in `spatial_coverage.py`).

The four bands, in descending reliability:

- **`scope_tag`** — the INSPIRE "Spatial scope" codelist (most deliberate) - from "raw_mcf" column
- **`place_keyword`** — the spatial keyword group (type "place" **or** a
  Country/Region/Continents vocabulary) - from "raw_mcf" column
- **`title`** — the title field (same trust rule as `place_keyword`).
- **`abstract`** — the abstract field (noisiest; countries & broad ignored).

## The reliability tiers, and why each is sticky (or not)

- **S3** (concrete pan-EU term, ≥2 countries, or `scope_tag: european/global`)
  → `for`, **sticky** — bbox cannot override.
- **S2** (exactly 1 country from a reliable band, or `scope_tag:
  national/regional/…`) → `against`, **sticky**.
- **S1** (a broad term, nothing stronger) → **bbox-arbitrated**.
- **S0** (nothing) → bbox decides.

### Why S3 is sticky

Copernicus/EEA datasets (*"High Resolution Layer: Imperviousness…"*) carry
`EEA39` as a place keyword plus the EEA template bbox `[-31.27, 27.64, -13.42,
66.57]` — a box that geometrically contains only Iceland, so it returns
`single_country_eu`. These records are genuinely pan-European; letting bbox
override `EEA39` would flip them to `against`. So a concrete pan-EU term from a
deliberate provenance wins regardless of bbox. This same record is the
canonical worked example: `place_keyword` → S3 `for` (`evidence="eea39"`), bbox
→ `against`, resolution rule 1 fires → `is_multi_country_eu=True`, surfaced as
`agree="disagree"` for review.

### Why S2 is sticky

A single named country from a reliable band (`place_keyword`, `title`, or
`spatial_desc`) — or an explicit `scope_tag: national` — is usually the
dataset's true scope, *because* the competing "multi" signal is typically a
garbage template bbox: the EEA mid-Atlantic box `[-31.27, 27.64, -13.42,
66.57]`, or the France BDGSF box `[-54.52, 2.05, 9.56, 51.15]` on French
national records. Letting bbox override would flip genuine national datasets
to `for`.

## Band-specific trust rules, and why

- **In abstract: countries & broad terms are ignored** (only a concrete acronym survives,
  demoted to S1). *Because* abstract country names are mostly affiliation,
  homonym, or multi-context noise: *"Geonor AS, Norway"* (affiliation),
  *"Norway Spruce"* [Picea abies] (homonym), *"Africa SoilGrids"* →
  `netherlands` = ISRIC (affiliation). 
  Broad terms hit journal citations (*"European Journal of Soil Science"*) and URLs. 

- **`scope_tag` can only be broad.** *Because* the "Spatial scope" vocabulary
  is a controlled codelist of scope *granularities*
  (`Global/European/National/Regional/Local/Project`) — we read the codelist,
  never mine the adjective from free text, so journal names cannot fire.
  Country names and EEA-style acronyms live in *other* vocabularies →
  `place_keyword`.

- **`place_keyword` is "spatial group", not "place type".** The rule is
  `keywords_type == "place"` **or** vocabulary ∈ {`Country`, `Region`,
  `Continents, countries…`}; `"individual"` is excluded. *Because* relying on
  the type alone misses genuine national tags that live in a `Country`
  vocabulary typed `(none)` (e.g. *Carte de la Réserve Utile… France*), while
  the type half alone would also pull in GEMET/AGROVOC topical keywords.

- **`spatial_desc` reuses the reliable-band trust rule** — it is a raw,
  provider-authored scope string in its own column, so it is treated as a fifth
  reliable band (mechanism in `spatial_coverage.py`).

Non-spatial keyword groups (theme/untyped + GEMET/AGROVOC) are dropped — they
carry negligible EU signal and are where funding/initiative names would leak.

## Why S1 is bbox-arbitrated, and the `global`/`worldwide` handling

A broad term (`europe`, `european`) is ambiguous: spatial in *"long-term
experiments in Europe"*, thematic in *"European Commission"*. With nothing
stronger, bbox is the tiebreaker (S1+bbox-for → `for`; S1+bbox-against →
`against`; S1+bbox-silent → the broad word stands). A broad term is also
*below* S2: a single country from a reliable band wins over it, so a national
dataset tagged `europe` stays `against` — which is why `europe` is not sticky.

`global`/`worldwide` is broad too: it does not enumerate a European geography
the way `eea39` does, so it is S1 (bbox-arbitrated), not S3-sticky. But a global
scope **is** multi-country-EU — it covers ≥2 European countries by definition —
so a `global`/`worldwide` occurrence is a `for` signal, and at bbox-silent the
broad word stands as `for`, exactly like `europe`. 
Bbox remains the safety net for a `global` tag on a
genuinely local dataset: a concrete contradicting bbox arbitrates it down.

The one place `global`/`worldwide` *is* handled specially is the **title**: in
a title it is frequently part of a thematic compound, not a scope word —
*"Global Warming Potential"*, *"Global Change"*, *"A Global Dilemma"*. That is
a *signal-extraction* problem (the title makes no scope claim there), not a
quality judgment: `mcf_parser` masks `global`/`worldwide` when it is followed by
a thematic modifier (the `DEFAULT_GLOBAL_THEMATIC_MODIFIERS` lexicon) before
broad-term matching, so a genuine occurrence (*"Global Soil Erosion"*) survives
and a thematic one does not register. 

The alternative considered was to *delete* `global`/`worldwide` from the term
set, but that throws away records where `Global` is a deliberate place keyword
or a `scope_tag`. Treating it as a broad term keeps every occurrence and lets
bbox arbitrate the ambiguous ones, the same mechanism that handles `europe`.

## Known residual (see notebook) — `scope_tag: project`

`project` resolves `against` (project-scale = sub-multi-country). Most
project-tagged records are genuinely single-project case studies (*SERENA EJPSOIL
PL*, *EJPSOIL ARTEMIS*) — correctly `against`. But some are project
*deliverables that aggregate multi-country data* and are false negatives:
*"A stocktaking of European mid- and long-term experiments"*. This is the
single best target for the next iteration — e.g. letting a `scope_tag:
european` or a pan-European title rescue a `project`-tagged record.
