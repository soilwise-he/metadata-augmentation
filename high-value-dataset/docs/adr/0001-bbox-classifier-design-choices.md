# 0001 — Bbox classifier: design choices

The classifier labels a WGS 84 bounding box as `single_country_eu`,
`multi_country_eu`, `non_european`, or `invalid`. This document records the
non-obvious architectural *whys* behind the pipeline. The mechanism (what each
step does, in what order) lives in the `bbox_classifier.py` module docstring;
a visual flowchart of that mechanism lives in
[`bbox-classifier-flowchart.md`](../bbox-classifier-flowchart.md). This file is
organized to mirror that pipeline, step by step.

## Language and invariants

The four labels (`single_country_eu`, `multi_country_eu`, `non_european`,
`invalid`) are defined in the `bbox_classifier.py` module docstring.

- **`_eu` and "EU set" mean the European continent, not the European  Union.**
  The set includes non-EU-member states such as Norway, Switzerland, the UK,
  Turkey, Iceland, and Cyprus. 
- A bbox resolves to **exactly one** of the four labels.
- **coverage vs share** — same numerator (a country's intersection area with the
  bbox), different denominator. *coverage* = `÷ country_total_area` (what
  fraction of the *country* is inside the bbox); *share* =
  `÷ total_EU_intersection_area` (what fraction of the *EU overlap* is this
  country). They are not interchangeable.

> **Dev:** "This Africa bbox returns `non_european`, but it clips Malta at 100%.
> Why isn't it a **single-country bbox**?"
> **Domain expert:** "Because the Malta overlap is *accidental* — the dataset is
> about Africa. A bbox only counts as **European** when the overlap is
> *intentional*. The Step 4 relevance test decides this, based on how much of each
> European country is actually inside the bbox."

## Step 0 — Country set: who counts as "European"

The EU set is `(continent == "Europe" AND status == "Member State") ∪ {Turkey,
Cyprus}`. Turkey and Cyprus are `continent == "Asia"` in the source GeoJSON but
are European states, so they are included by name.

**Russia is clipped to longitudes `[0°, 60°E]`** (west of the Urals) and lives in the EU set (clipped).
- *Why clip:* Russia's full geometry would make any Siberian bbox intersect
  Russia and classify as European. Clipping means a Siberian bbox produces 0 EU
  hits → `non_european`, while Moscow still hits clipped Russia.
- *Why the lower bound is 0°, not −180°:* Russia's source geometry crosses the
  antimeridian — its far east (Chukotka) is stored at negative longitude, so a
  clip to `[−180, 60]` leaks that far-eastern tail back in and pushes the
  clipped bbox's western edge out to −180. Clipping to `[0, 60]` drops it.
  Nothing of European Russia is lost.


## Step 1.0 — Validation gate: a separate `invalid` label

The `spatial` column carries ~25 dataset records with projected/metric coordinates
or swapped axes.`invalid` means "we can't trust the input" (input validation);  

Cleaning (reproject / unswap) belongs upstream in the harvest/ingest pipeline, where the source CRS is known — not inside the classifier. The gate rejects coordinates
out of WGS 84 range, `east < west`, and `south > north` *before* any geometry
work, using strict `<` so degenerate bboxes pass through to Step 1.5.

A legitimate antimeridian-crossing bbox (e.g. `170, 60, -170, 70`) has
`east < west` and is therefore returned as `invalid`.

## Step 1.5 — Degenerate bboxes (points and lines) handled explicitly

Every downstream filter is area-based; a point or line intersection with a
country has area 0, so the `area > 0` guards would drop every hit and a point in
central Paris would return `non_european`. 

Points resolve by containment (`covers`, not `contains`, so a point exactly on a national border is still claimed); 
lines resolve by EPSG:3035 intersection length.
Containment is exclusive (a point sits in at most one country), so there is no
EU/non-EU competition and the relevance test never runs for points or lines.

## Steps 2–3 — CRS choice: intersect in EPSG:4326, measure area in EPSG:3035

Intersections are computed in EPSG:4326 (the source CRS), then each intersection
is reprojected to EPSG:3035 (ETRS89 / LAEA Europe, equal-area) to measure area (or length). 
EPSG:3035 is a projection optimized to preserve area size in Europe.

Doing it all in EPSG:3035 was tried and **wrongfully excluded countries** 
for large (e.g. worldwide) bboxes: reprojecting the raw bbox polygon
to 3035 before intersecting distorts it enough that some countries no longer
overlap. Intersecting first in 4326 (where overlap is exact) and measuring only
the *result* in 3035 (where area is accurate) avoids both distortion and
area-error. Intersection emptiness is projection-independent, so the non-EU
boolean check is also safe to run in 4326 only.

## Step 4 — Relevance test: intersect all countries, not just the EU set

Why does this test exist? Because checking **only** EU countries is not enough.
A bbox about a non-EU neighbour clips the EU country on its border; that border
country is the *only* EU country hit, so it accounts for 100% of the EU overlap
and the naive EU-only pipeline labels the bbox `single_country_eu`. A Syria bbox
hits Turkey; an Iraq bbox hits Turkey; a Kazakhstan bbox hits (clipped) Russia.
Each is genuinely about somewhere else, yet each would be called European. The
relevance test exists to separate these **accidental** border clips from
**intentional** European content.

It does so by intersecting **all** countries, not just the EU set: the non-EU
overlap is what reveals intent. A bbox about Spain has Spain ~100% covered and
Algeria/Morocco only partially; a bbox about Syria has Syria ~100% covered and
a small part of Turkey. 

The test runs only on **Path B** — bboxes that hit both EU and non-EU countries.
A **Path A** bbox (EU-only hits) has no competition, so it skips this test and
goes straight to Step 5.

**The test:** `coverage ≥ 50% AND collective_share ≥ 10%`, where:

- **coverage** of a country = `intersection_area / total_country_area` — the
  fraction of the country that sits inside the bbox, measured against the
  *country*, not the bbox. A country is **meaningful** when its coverage ≥ 50%.
- **collective_share** = the meaningful countries' combined intersection area as
  a fraction of the total EU intersection area.

Both gates are essential:

- *coverage ≥ 50%:* at least one EU country must be substantially captured, not
  merely border-clipped.
- *collective_share ≥ 10%:* the meaningful countries must weigh enough that tiny
  islands aren't mistaken for intent. The share is *summed* across all meaningful
  countries rather than checked per-country: the Africa bbox has Malta + Cyprus
  both 100% covered yet together only 7.6% of the EU intersection area (Turkey
  dominates despite ~9% coverage), so summing groups them below the 10% bar and
  the bbox is correctly rejected. On the acceptance side, the European
  bbox (`[-30, 34, 60, 72]`) covers all 45 EU countries at ≥50% each, so their
  shares sum to ~100% and pass trivially.

Intersecting all ~256 countries sounds expensive, but each
country is first screened by a `.bounds` overlap check — pure coordinate
arithmetic on a cached rectangle — so only the handful whose bounding box
overlaps the query bbox ever reach polygon intersection. That same `.bounds`
loop is not throwaway work: the countries that pass become `eu_candidates`, the
exact list Step 3 intersects for real. So one cheap pass both rejects bboxes
that cannot hit any EU country (`eu_candidates` empty → `non_european`) and
builds the work list for the next step. Non-EU candidates are checked
boolean-only — first hit wins, no EPSG:3035 reprojection — since Step 4 only
needs to know *whether* any non-EU country is hit, not how much.


## Step 5 — Single vs multi: dominance, majority, and the Russia special-case

Rule 1 and rule 2 read complementary signals, so neither subsumes the other. Rule 1
is *share/area*-centric — it resolves the country that owns the bulk of the
EU-overlap pie. Rule 2 is *coverage-fraction*-centric — it resolves the country
that is far more *complete* inside the bbox than its neighbours. Each guards a
national-bbox shape the other is structurally blind to:

- A **large** subject's bbox almost always fully contains a microstate or small
  neighbour (Monaco, Andorra, San Marino, and Luxembourg sit at ~100% coverage
  inside France's, Italy's, and Germany's bboxes), which collapses rule 2's
  coverage ratio below its threshold — only rule 1's majority gate resolves it.
- A **small** subject fully inside a bbox dominated by a larger neighbour's raw area never reaches a
 majority. E.g. the bbox around Luxemburg contains large parts of its neighboring countries — only rule 2's coverage ratio resolves it. 


A country resolves `single_country_eu` in rule 1 when it dominates the EU
intersection by raw area (`largest / runner_up > _RAW_DOMINANCE_CUTOFF`,
default 2) **or**, for polygons, when it holds a majority of the EU
intersection area (`share >= _SINGLE_COUNTRY_SHARE_THRESHOLD`, 0.50). The two
gates are OR-ed because each catches real national-bbox misclasses the other
misses: a ratio-dominant bbox whose subject country sits just under the majority
line, and a majority-dominant bbox that fails the ratio gate. The 0.50 reuses
the same "is-this-country-really-the-subject?" majority as the Step 4 relevance
test, over a different denominator (share of the EU intersection vs coverage of
a single country). The majority gate is skipped for line bboxes, which stay on
the raw-length-dominance rule.

Two real edge cases still force extra handling on top of the ratio gate:

- **Russia pan-European override.** Russia is so large that it dominates by raw
  area in pan-European bboxes where it is *not* the subject, so when Russia is
  the largest intersection without a majority (`share < 0.50`) the result is
  forced to `multi_country_eu` regardless of the ratio. This replaces an earlier
  country-count threshold — a coarser proxy for "pan-European" that could not
  distinguish pan-European from Russia-focused bboxes.
- **Coverage-dominance fallback** (polygons only). A small country entirely
  inside the bbox (e.g. a Luxembourg bbox) may not dominate by raw area but is
  *fully* covered while its larger neighbours are barely clipped. Comparing
  *coverage fractions* (intersection / total country area) instead of raw area
  catches this — but coverage alone is not enough. "Fully covered" is cheap for
  any microstate that merely sits inside a larger subject's bbox (Luxembourg is
  fully inside a Benelux bbox too), so the coverage ratio is gated by a second
  check: the top-coverage country must also hold more than
  `_COVERAGE_FALLBACK_MIN_SHARE` (10%) of the EU intersection area — *does that
  country actually carry weight in the bbox?* This reuses the Step 4
  coverage-vs-share duality, down to the same 10% floor
  (`_COLLECTIVE_SHARE_THRESHOLD`): coverage asks "is one country far more
  *fully* captured than the others?", share asks "and does it matter?". This step is
  skipped for lines, where length relative to total area is meaningless.

## Accepted limitations

These are borderline calls — the assigned label is defensible but a reasonable
alternative exists. They span both boundaries: the EU/non-EU line (a
`non_european` result that could be European) and the single/multi line (a
`single_country_eu` or `multi_country_eu` result that could go the other way).
All are niche and accepted given the European-mainland focus; where a threshold
fix is possible, the entry notes why it is not worth the regression it would
cause.

- **Microstates / overseas territories.** Holy See (Vatican) is a `Permanent
  Observer`, not a `Member State`, so it is not in the EU set; a Vatican-only
  bbox hits Italy (<1%) + Holy See (non-EU) → `non_european`. Madeira, the
  Azores, and Greenland are separate `* Territory` entries (not mainland),
  likewise excluded because we selected `Status == member state`. 
- **Thin latitudinal transect.** A thin strip across many EU countries where no
  single one reaches 50% coverage (e.g. `[9.87, 50.19, 51.64, 51.64]` — Poland
  30.8%, Czechia 22%, Ukraine 18.5%) is rejected as `non_european`. This is a
  structural blind spot of the coverage gate; fixing it with a "distributed
  coverage" heuristic would re-break the Kazakhstan/Syria rejections.
- **Line straddling an EU/non-EU border.** A line that runs mostly through a
  non-EU country but clips an EU border resolves to its single EU country (the
  one real case is `[54.06, 12.09, 54.06, 54.76]` → Russia, 10.2% EU).
- **In the Notebook — a genuine multi-country bbox collapsed to single** (from
  `bbox_analyses.ipynb`). `[5.866944, 46.371944, 17.160556, 55.059167]` —
  this bbox is actually about Germany AND Austria (12%), yet it resolves
  `single_country_eu` because Germany clears **both** rule-1 gates: share
  0.5119 ≥ `_SINGLE_COUNTRY_SHARE_THRESHOLD` (0.50) *and* raw ratio 4.26 >
  `_RAW_DOMINANCE_CUTOFF` (2) (rule 1 is an OR, so either alone suffices).
  Lowering the share threshold would not help — the ratio gate still fires
  single. Experimenting with different threshold, always returned more misclassifications.
- **A country's own rectangular bbox can resolve `multi_country_eu` when the
  bbox is a poor proxy for the country's shape** (`test_bbox_classifier.py`,
  `_OWN_BBOX_IS_MULTI`). These are *structural* — not threshold-fixable.
  - *Norway* `[4.78968, 57.98791, 31.07354, 71.15471]` → multi. The bbox
    reaches far east (to 31.1°E) and encloses most of Sweden and the whole of Finland;
    Norway is only 27% of the intersection (Sweden 32%, Finland 28%), and
    coverage_ratio is 1.00 — Norway does not lead on coverage either.
  - *Croatia* `[13.50479, 42.39999, 19.42500, 46.53583]` → multi. Croatia
    wraps around Bosnia & Herzegovina (in the EU set under the continent
    convention), so its bbox is about as much Bosnia as Croatia: Croatia 36%,
    Bosnia 33%, Slovenia 11%; coverage_ratio 1.01.
  - *Why accepted:* These misclassifications are hard to classify by the human eye as well.
    Neither a share nor a coverage threshold recovers these —
    the country is not the majority of its own bbox. 

