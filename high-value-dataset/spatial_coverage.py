"""Classify each dataset record's spatial extent as multi-country-European.

This module fuses three signals — each an independent claim about the record's
spatial coverage — into a single boolean ``is_multi_country_eu``:

- the bounding-box verdict from :mod:`bbox_classifier`, which labels a bbox
  geometrically as ``single_country_eu`` / ``multi_country_eu`` /
  ``non_european`` / ``invalid``;
- the free-text ``spatial_desc`` field of the df; and
- the EU-related terms extracted from keywords / title / abstract by
  :mod:`mcf_parser` (carried in the ``eu_related_terms`` column).


The specificity ladder
----------------------
Each record carries three signals, each with a vote — ``for`` (multi-country-EU),
``against`` (not), or ``silent`` (no usable claim). The verdict is resolved by a
**specificity ladder**: the most specific *text* signal wins, and the bbox 
decides only when text is silent (or, for the weakest text tier). 

STEP 1 — CLASSIFY THE TWO TEXT SIGNALS (same 4-tier scheme)
    Both text sources (keywords and "spatial_desc" column) are ranked into one of four tiers. 
    The pooled text tier is the **highest** of the two.

    =======  =================================================  ========  ===========
    tier     text contains…                                      vote      specificity
    =======  =================================================  ========  ===========
    **S3**   a concrete multi-country term (``eea39``, ``eu27``) ``for``    highest
             **or** ≥2 European countries **or** ``worldwide``/``global``
    **S2**   exactly 1 European country                           ``against``
    **S1**   a broad EU term (``europe``, ``europa``,             ``for``    weak
             ``european union``) and nothing stronger
    **S0**   nothing                                             ``silent``
    =======  =================================================  ========  ===========

    The two sources are:

    - ``eu_related_terms`` — the dict extracted by :mod:`mcf_parser`, carrying up
      to three keys: ``concrete_multi_country_terms`` (S3), ``broad_eu_terms``
      (S1), ``eu_countries`` (S2). MCF-keyword and title/abstract evidence are
      already pooled within each key.
    - ``spatial_desc`` — a raw provider-authored scope string, split on commas
      and routed through the same tier scheme.

    Broad and concrete terms are matched with **word boundaries**
    (``(?<![a-z])…(?![a-z])``) and keywords by exact equality, so a term never
    fires inside ``European Commission``, a ``.eu`` URL, or ``europium``. Vague
    bare ``eu`` / ``europ`` substrings are **not** evidence (funding bodies,
    journal names, French grammar, chemistry) and are not collected. *Avoid:*
    relaxing the matching to a substring — it adds noise, not signal.

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
    1. Pooled text tier **S3** → **for**. *(bbox cannot override a concrete
       pan-European claim — e.g. ``EEA39``.)*
    2. Pooled text tier **S2** → **against**. *(a single country is sticky — it
       is the dataset's scope; the competing "multi" signal is usually a garbage
       template bbox.)*
    3. Pooled text tier **S1** → **bbox arbitrates**: bbox ``for`` → **for**,
       bbox ``against`` → **against**, bbox ``silent`` → **for** *(the broad word
       stands unless concrete geometry contradicts it)*.
    4. Pooled text tier **S0** → **bbox decides**: bbox ``for`` → **for**;
       otherwise **against**.

STEP 4 — AGREEMENT
    ``agree`` reports the relationship between the non-silent votes (among
    ``bbox_vote``, ``spatial_desc_vote``, ``terms_vote``):

    - ``"agree"`` — ≥2 votes active, all the same.
    - ``"disagree"`` — any two active votes differ.
    - ``""`` (empty) — fewer than 2 votes active (nothing to compare).

    This flag will allow us to study the cases where two "sources of truth" disagree
    and improve our algorithm accordingly.

The EU geography (country set, aliases, concrete/broad terms) comes from
:mod:`eu_terms_and_countries` — the single source of truth shared with
:mod:`bbox_classifier`, so a Norway-only record is treated identically by its
bbox and its text.
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


def _classify_text_source(concrete, broad, countries):
    """Map one text source's three buckets to ``(tier, vote, evidence)``.

    Implements STEP 1 for a single source. *evidence* is the ``"; "-joined``
    token(s) at the winning tier — the concrete term(s), the country list, or the
    broad term(s). It is ``""`` for S0.

    A concrete term and ≥2 countries both yield S3; when both are present the
    concrete term wins the *evidence* (it is the more deliberate scope label).
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
                seen.add(canonical)
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
    """STEP 3 — the verdict, from the pooled text tier and the bbox vote."""
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
        Aligned to ``df.index``. ``df`` is **not** mutated. Columns:

        - ``is_multi_country_eu`` (bool) — the verdict.
        - ``bbox_vote``, ``spatial_desc_vote``, ``terms_vote`` (str) — each
          ``"for"`` / ``"against"`` / ``"silent"``.
        - ``agree`` (str) — ``"agree"`` (≥2 non-silent votes concur),
          ``"disagree"`` (any two differ), or ``""`` (fewer than 2 non-silent
          votes — nothing to compare).
        - ``terms_evidence`` (str) — the token(s) at the *terms* signal's winning
          tier (``""`` for S0). When this is empty but ``text_tier`` is high, the
          tier was driven by ``spatial_desc`` (visible in ``df``).
        - ``text_tier`` (str) — the pooled text tier, ``"S0"``–``"S3"``.
        - ``bbox_classification`` (str or None) — the raw bbox label.
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
        if isinstance(terms_cell, dict):
            t_concrete = terms_cell.get("concrete_multi_country_terms", [])
            t_broad = terms_cell.get("broad_eu_terms", [])
            t_countries = terms_cell.get("eu_countries", [])
        else:
            t_concrete, t_broad, t_countries = [], [], []
        terms_tier, terms_vote, terms_evidence = _classify_text_source(
            t_concrete, t_broad, t_countries,
        )

        s_concrete, s_broad, s_countries = _spatial_desc_buckets(
            row.get(spatial_desc_col), concrete_terms, broad_terms,
            eu_countries, eu_country_aliases,
        )
        spatial_tier, spatial_vote, _ = _classify_text_source(
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
