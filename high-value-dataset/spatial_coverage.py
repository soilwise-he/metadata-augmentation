"""Classify whether each dataset record's spatial extent is multi-country-European.

This module fuses three signals — each an independent claim about the record's
spatial coverage — into a single boolean ``is_multi_country_eu``:

- the bounding-box verdict from :mod:`bbox_classifier`, which labels a bbox
  geometrically as ``single_country_eu`` / ``multi_country_eu`` /
  ``non_european`` / ``invalid``;
- the free-text ``spatial_desc`` field of the df; and
- the EU-related terms extracted by :mod:`mcf_parser` (carried in the
  ``eu_related_terms`` column), **tagged by provenance** — *where* each term
  was extracted from.


The ``eu_related_terms`` cell
-----------------------------
A nested dict keyed by **provenance band** (produced by
:meth:`mcf_parser.McfParser.extract("eu_related")`). Each band is present only
when it carries signal; the whole cell is ``None`` when nothing matched. The
inner shape is **not uniform** — ``scope_tag`` carries scope *granularities*
(the INSPIRE codelist values), while the other three bands carry matched term
*buckets*. That asymmetry is why each band needs its own trust rule (STEP 1):

::

    {
      "scope_tag":     {"for": [...], "against": [...]},   # codelist values
      "place_keyword": {"concrete_multi_country_terms": [...],
                        "broad_eu_terms": [...],
                        "eu_countries": [...]},
      "title":         {<same three keys as place_keyword>},
      "abstract":      {<same three keys as place_keyword>},
    }

``scope_tag.for`` / ``.against`` are the codelist values that vote for / against
multi-country-EU (``european``/``global`` vs ``national``/``regional``/…). The
three buckets come from :mod:`mcf_parser`: ``concrete_multi_country_terms``
(e.g. ``eea39``), ``broad_eu_terms`` (e.g. ``europe``), ``eu_countries``
(canonical country names). A cell that is missing or not a dict is treated as
S0 — text-silent.


The reliability ladder
----------------------
Each record carries three signals, each with a vote — ``for`` (multi-country-EU),
``against`` (not), or ``silent`` (no usable claim). The verdict is resolved by a
**reliability ladder**: the most reliable *text* signal wins, and the bbox
decides only when text is silent (or, for the weakest text tier). 

STEP 1 — CLASSIFY THE TEXT SIGNALS, GATED BY PROVENANCE
    Reliability is a function of two axes — *where* the term was extracted
    (its provenance band) and *what* it denotes (its specificity).
    ``eu_related_terms`` is nested by **band** (see ADR 0002); each band
    is ranked into one of four tiers under a band-specific trust rule, and the
    pooled terms tier is the **highest** across bands (and ``spatial_desc``).

    The four bands (where does the term come from?), in descending reliability:

    ===================  ==========================================  ============================
    band                 source                                      trust rule
    ===================  ==========================================  ============================
    ``scope_tag``        INSPIRE "Spatial scope" codelist keyword    ``european``/``global`` → S3 for;
                                                                     other (national/…) → S2 against
    ``place_keyword``    spatial keyword group                       concrete / ≥2 countries → S3 for;
                         (type "place" OR Country/Region/… vocab)    1 country → S2 against; broad → S1
    ``title``            title field                                 (same rule as place_keyword)
    ``abstract``         abstract field                              concrete → S1 for;
                                                                     **countries & broad ignored**
    ===================  ==========================================  ============================

    The four tiers:

    =======  =================================================  ========  ===========
    tier     text contains…                                      vote      reliability
    =======  =================================================  ========  ===========
    **S3**   a concrete multi-country term (``eea39``, ``eu27``) ``for``    highest
             **or** ≥2 European countries **or** a scope_tag
             ``european``/``global``
    **S2**   exactly 1 European country                           ``against``
             **or** a scope_tag ``national``/``regional``
    **S1**   a broad EU term (``europe``, ``european``,           ``for``    weak
             ``global``, ``worldwide``) and nothing stronger
    **S0**   nothing                                             ``silent``
    =======  =================================================  ========  ===========

    The *why* behind each band's trust rule is recorded in ADR 0002.

STEP 2 — CLASSIFY THE BBOX
    From the raw dict produced by :mod:`bbox_classifier`:

    =========================  ===========
    bbox classification        bbox vote
    =========================  ===========
    ``multi_country_eu``       ``for``
    ``single_country_eu``      ``against``
    ``non_european``           ``against``
    ``invalid`` / ``None``     ``silent``
    =========================  ===========

STEP 3 — RESOLVE THE VERDICT (first rule that fires)
    The **pooled text tier** is the strongest text signal across the four
    provenance bands *and* ``spatial_desc`` (S0–S3); the first matching rule
    fires. See :func:`classify_multi_country_eu` for how it is pooled and which
    output columns expose which part.

    1. Pooled text tier **S3** → **for**. *(bbox cannot override a concrete
       pan-European claim — e.g. ``EEA39``.)*
    2. Pooled text tier **S2** → **against**. *(a single country is sticky — it
       is the dataset's scope; the competing "multi" signal is usually a garbage
       template bbox.)*
    3. Pooled text tier **S1** → **bbox arbitrates**: bbox ``for`` → **for**,
       bbox ``against`` → **against**, bbox ``silent`` → **for** *(the broad word
       stands unless concrete geometry contradicts it)*. ``global``/``worldwide``
       is a broad term like any other; however thematic title phrases (``"global
       warming"``) are masked in :mod:`mcf_parser` and never reach S1. 
    4. Pooled text tier **S0** → **bbox decides**: bbox ``for`` → **for**;
       otherwise **against**.

STEP 4 — AGREEMENT
    ``agree`` reports the relationship between the non-silent votes (among
    ``bbox_vote``, ``spatial_desc_vote``, ``terms_vote``):

    - ``"agree"`` — ≥2 votes active, all the same.
    - ``"disagree"`` — any two active votes differ.
    - ``""`` (empty) — fewer than 2 votes active (nothing to compare).

    This flag will allow us to study the cases where two signals disagree
    and improve our algorithm accordingly.

The EU geography (country set, aliases, concrete/broad terms) comes from
:mod:`eu_terms_and_countries` — the single source of truth, shared with
:mod:`bbox_classifier`.
"""

import pandas as pd

from eu_terms_and_countries import (
    DEFAULT_EU_COUNTRIES,
    DEFAULT_EU_COUNTRY_ALIASES,
    DEFAULT_CONCRETE_MULTI_COUNTRY_EU_TERMS,
    DEFAULT_BROAD_EU_TERMS,
)

# Specificity ranks (higher = more decisive). The pooled text tier is the max.
_TIER_RANK = {"S0": 0, "S1": 1, "S2": 2, "S3": 3}

_VOTE_FOR = "for"
_VOTE_AGAINST = "against"
_VOTE_SILENT = "silent"


def _classify_reliable_band(concrete, broad, countries):
    """The trust rule for reliable bands (place_keyword, title, spatial_desc).

    Maps the three buckets to ``(tier, vote, evidence)``: 
    a concrete term or ≥2 countries → S3 sticky-``for``; 
    exactly 1 country → S2 sticky-``against``;
    a broad term → S1 bbox-arbitrated ``for``; 
    otherwise S0 silent. 
    *evidence* is the ``"; "-joined`` token(s) at the winning tier (``""`` for S0). When
    both a concrete term and ≥2 countries are present, the concrete term wins
    the *evidence* (it is the more deliberate scope label).
    """
    concrete = list(concrete or [])
    broad = list(broad or [])
    countries = list(countries or [])
    if concrete or len(countries) >= 2:
        evidence = "; ".join(concrete) if concrete else "; ".join(countries)
        return "S3", _VOTE_FOR, evidence
    if len(countries) == 1:
        return "S2", _VOTE_AGAINST, countries[0]
    if broad:
        return "S1", _VOTE_FOR, "; ".join(broad)
    return "S0", _VOTE_SILENT, ""


def _classify_abstract_band(concrete, broad, countries):
    """The trust rule for the abstract band.

    Only a concrete acronym survives, demoted to S1 bbox-arbitrated; countries
    and broad terms are ignored (S0). See ADR 0002 for why the abstract band is
    treated as noisy.
    """
    concrete = list(concrete or [])
    if concrete:
        return "S1", _VOTE_FOR, "; ".join(concrete)
    return "S0", _VOTE_SILENT, ""


def _classify_scope_tag(for_values, against_values):
    """The trust rule for the scope_tag band (INSPIRE ``Spatial scope`` codelist).

    The codelist carries scope *granularities*, never countries or acronyms. 
    A ``for`` value (``european``/``global``) → S3 sticky-``for``; 
    any other value (``national``/``regional``/…) → S2 sticky-``against``. 
    """
    for_values = list(for_values or [])
    against_values = list(against_values or [])
    if for_values:
        return "S3", _VOTE_FOR, "; ".join(for_values)
    if against_values:
        return "S2", _VOTE_AGAINST, "; ".join(against_values)
    return "S0", _VOTE_SILENT, ""


def _pool_bands(band_results):
    """Return the ``(tier, vote, evidence)`` at the highest tier across bands.

    The pooled terms tier is the max across the provenance bands; ties resolve
    to the first band at that tier (order: scope_tag, place_keyword, title,
    abstract — most reliable first).
    """
    active = [r for r in band_results if r[0] != "S0"]
    if not active:
        return "S0", _VOTE_SILENT, ""
    return max(active, key=lambda r: _TIER_RANK[r[0]])


def _spatial_desc_buckets(desc, concrete_terms, broad_terms, eu_countries, aliases):
    """Split a free-text ``spatial_desc`` into the same three buckets.

    Comma-separated. Each part routes to ``concrete`` / ``broad`` / ``country``
    or is ignored. ``"other"`` and empty parts contribute nothing → S0.
    """
    if not isinstance(desc, str) or not desc.strip():
        return [], [], []
    lower = desc.strip().lower()
    concrete, broad, countries = [], [], []
    seen = set()
    for part in lower.split(","):
        part = part.strip()
        if not part:
            continue
        if part in concrete_terms:
            if part not in concrete:
                concrete.append(part)
        elif part in broad_terms:
            if part not in broad:
                broad.append(part)
        else:
            canonical = aliases.get(part, part)
            if canonical in eu_countries and canonical not in seen:
                countries.append(canonical)
                seen.add(canonical) #ensures each country name lands in "countries" only once, even if mentioned mulitple times in the text
    return concrete, broad, countries


def _bbox_vote(result):
    """Map a bbox_classifier result dict (or ``None``) to ``(vote, classification)``."""
    if result is None:
        return _VOTE_SILENT, None
    label = result.get("classification")
    if label == "multi_country_eu":
        return _VOTE_FOR, label
    if label in ("single_country_eu", "non_european"):
        return _VOTE_AGAINST, label
    return _VOTE_SILENT, label  # invalid / unknown


def _resolve(pooled_tier, bbox_vote):
    """STEP 3 — the verdict, from the pooled text tier and the bbox vote.

    S3 → ``for``; S2 → ``against``; S1 → bbox-arbitrated (bbox-silent → ``for``;
    the broad word stands unless concrete geometry contradicts it); S0 → bbox
    decides. ``global``/``worldwide`` is a broad term like any other — see ADR
    0002 for why it is not special-cased.
    """
    if pooled_tier == "S3":
        return True
    if pooled_tier == "S2":
        return False
    if pooled_tier == "S1":
        if bbox_vote == _VOTE_FOR:
            return True
        if bbox_vote == _VOTE_AGAINST:
            return False
        return True  # bbox silent → the broad word stands
    # S0 → bbox decides
    return bbox_vote == _VOTE_FOR


def _agree(bbox_vote, spatial_desc_vote, terms_vote):
    """STEP 4 — agreement status across the non-silent votes.

    ``"agree"`` when ≥2 votes are active and all concur, ``"disagree"`` when any
    two differ, and ``""`` when fewer than 2 votes are active (no pairwise
    comparison possible).
    """
    active = [v for v in (bbox_vote, spatial_desc_vote, terms_vote)
              if v != _VOTE_SILENT]
    if len(active) < 2:
        return ""
    return "agree" if len(set(active)) == 1 else "disagree"


def classify_multi_country_eu(df, bbox_results=None,
                               eu_related_terms_col="eu_related_terms",
                               spatial_desc_col="spatial_desc",
                               eu_countries=None, eu_country_aliases=None,
                               concrete_multi_country_eu_terms=None,
                               broad_eu_terms=None):
    """Fuse three signals into a per-record multi-country-EU verdict.

    Parameters
    ----------
    df : pandas.DataFrame
        Rows describing dataset records. Must contain the ``eu_related_terms``
        and ``spatial_desc`` columns (see the module docstring for the
        ``eu_related_terms`` dict shape, produced by
        :meth:`mcf_parser.McfParser.extract("eu_related")`).
    bbox_results : list of dict or None, default ``None``
        The raw classification dicts from
        :func:`bbox_classifier.classify_bboxes`, aligned to ``df``. ``None`` (or
        a ``None`` entry) → that record's bbox vote is ``silent`` (text-only
        mode).
    eu_related_terms_col, spatial_desc_col : str
        Column names.
    eu_countries, eu_country_aliases, concrete_multi_country_eu_terms, broad_eu_terms :
        Optional overrides for the EU geography reference sets. Default to the
        canonical sets from :mod:`eu_terms_and_countries`.

    Returns
    -------
    pandas.DataFrame
        Aligned to ``df.index``. Columns:

        - ``is_multi_country_eu`` (bool) — the verdict.
        - ``bbox_vote``, ``spatial_desc_vote``, ``terms_vote`` (str) — each
          ``"for"`` / ``"against"`` / ``"silent"``.
        - ``agree`` (str) — ``"agree"`` (≥2 non-silent votes concur),
          ``"disagree"`` (any two differ), or ``""`` (fewer than 2 non-silent
          votes — nothing to compare).
        - ``terms_evidence`` (str) — the token(s) at the *bands* pool's winning
          tier (``""`` for S0). Covers the four provenance bands only; see
          *Notes* for why this can be empty while ``text_tier`` is high.
        - ``text_tier`` (str) — the pooled text tier, ``"S0"``–``"S3"``.
        - ``bbox_classification`` (str or None) — the raw bbox label.

    Notes
    -----
    The verdict follows the STEP 3 ladder in the module docstring, combining
    ``text_tier`` with the bbox vote. ``text_tier`` is pooled in two steps:
    :func:`_pool_bands` takes the max tier across the four provenance bands
    (→ ``terms_vote`` / ``terms_evidence``), then ``max(terms_tier,
    spatial_tier)`` folds in ``spatial_desc``. Because ``terms_vote`` /
    ``terms_evidence`` cover the **four-band pool only**, ``spatial_desc`` is
    reported separately in ``spatial_desc_vote`` (it has no evidence column).
    So an empty ``terms_evidence`` with a high ``text_tier`` means the tier was
    driven by ``spatial_desc``.
    """
    eu_countries = eu_countries or DEFAULT_EU_COUNTRIES
    eu_country_aliases = eu_country_aliases or DEFAULT_EU_COUNTRY_ALIASES
    concrete_terms = (
        concrete_multi_country_eu_terms or DEFAULT_CONCRETE_MULTI_COUNTRY_EU_TERMS
    )
    broad_terms = broad_eu_terms or DEFAULT_BROAD_EU_TERMS

    if bbox_results is None:
        bbox_results = [None] * len(df)
    if len(bbox_results) != len(df):
        raise ValueError(
            f"bbox_results length ({len(bbox_results)}) != df length ({len(df)})"
        )

    records = []
    for (_, row), bbox_result in zip(df.iterrows(), bbox_results):
        terms_cell = row.get(eu_related_terms_col)
        band_results = []
        if isinstance(terms_cell, dict):
            st = terms_cell.get("scope_tag") or {}
            band_results.append(_classify_scope_tag(
                st.get("for"), st.get("against")))
            pk = terms_cell.get("place_keyword") or {}
            band_results.append(_classify_reliable_band(
                pk.get("concrete_multi_country_terms"),
                pk.get("broad_eu_terms"),
                pk.get("eu_countries")))
            tt = terms_cell.get("title") or {}
            band_results.append(_classify_reliable_band(
                tt.get("concrete_multi_country_terms"),
                tt.get("broad_eu_terms"),
                tt.get("eu_countries")))
            ab = terms_cell.get("abstract") or {}
            band_results.append(_classify_abstract_band(
                ab.get("concrete_multi_country_terms"),
                ab.get("broad_eu_terms"),
                ab.get("eu_countries")))
        terms_tier, terms_vote, terms_evidence = _pool_bands(band_results)

        s_concrete, s_broad, s_countries = _spatial_desc_buckets(
            row.get(spatial_desc_col), concrete_terms, broad_terms,
            eu_countries, eu_country_aliases,
        )
        spatial_tier, spatial_vote, _ = _classify_reliable_band(
            s_concrete, s_broad, s_countries,
        )

        pooled_tier = max((terms_tier, spatial_tier), key=lambda t: _TIER_RANK[t])

        bbox_vote, bbox_classification = _bbox_vote(bbox_result)
        verdict = _resolve(pooled_tier, bbox_vote)
        agree = _agree(bbox_vote, spatial_vote, terms_vote)

        records.append({
            "is_multi_country_eu": verdict,
            "bbox_vote": bbox_vote,
            "spatial_desc_vote": spatial_vote,
            "terms_vote": terms_vote,
            "agree": agree,
            "terms_evidence": terms_evidence,
            "text_tier": pooled_tier,
            "bbox_classification": bbox_classification,
        })

    return pd.DataFrame(records, index=df.index)
