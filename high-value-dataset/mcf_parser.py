import json
import re
import pandas as pd

from eu_terms_and_countries import (
    DEFAULT_EU_COUNTRIES,
    DEFAULT_EU_COUNTRY_ALIASES,
    DEFAULT_CONCRETE_MULTI_COUNTRY_EU_TERMS,
    DEFAULT_BROAD_EU_TERMS,
    DEFAULT_SCOPE_TAG_VOCABULARY,
    DEFAULT_SPATIAL_KEYWORD_VOCABULARIES,
    DEFAULT_SCOPE_TAG_FOR_VALUES,
    DEFAULT_GLOBAL_THEMATIC_MODIFIERS,
)


class McfParser:

    _COLUMN_NAMES = {
        "access_constraints": "access_constraints_mcf",
        "rights": "rights_mcf",
        "license": "license_mcf",
        "opendata_keyword": "has_opendata_keyword_mcf",
        "soil_related": "is_soil_related_mcf",
        "eu_related": "eu_related_terms",
    }

    DEFAULT_SOIL_TERMS = (
        "soil",
    )

    _TERM_BASED_FIELDS = {
        "soil_related": "_contains_soil_term",
    }

    def __init__(self, df, soil_terms=None,
                 concrete_multi_country_eu_terms=None,
                 broad_eu_terms=None, eu_countries=None,
                 eu_country_aliases=None,
                 scope_tag_vocabulary=None,
                 spatial_keyword_vocabularies=None,
                 scope_tag_for_values=None,
                 global_thematic_modifiers=None):
        self._df = df
        self._soil_terms = soil_terms or self.DEFAULT_SOIL_TERMS
        self._concrete_multi_country_eu_terms = (
            concrete_multi_country_eu_terms or DEFAULT_CONCRETE_MULTI_COUNTRY_EU_TERMS
        )
        self._broad_eu_terms = broad_eu_terms or DEFAULT_BROAD_EU_TERMS
        self._eu_countries = eu_countries or DEFAULT_EU_COUNTRIES
        self._eu_country_aliases = (
            eu_country_aliases or DEFAULT_EU_COUNTRY_ALIASES
        )
        self._scope_tag_vocabulary = (
            scope_tag_vocabulary or DEFAULT_SCOPE_TAG_VOCABULARY
        )
        self._spatial_keyword_vocabularies = (
            spatial_keyword_vocabularies or DEFAULT_SPATIAL_KEYWORD_VOCABULARIES
        )
        self._scope_tag_for_values = (
            scope_tag_for_values or DEFAULT_SCOPE_TAG_FOR_VALUES
        )
        # Regex that masks ``global``/``worldwide`` when it is a thematic modifier
        # (``global warming``, ``worldwide leaf …``) — not a spatial scope. Only the
        # modifier word is neutralized (→ ``xglobal``), so a genuine ``global``
        # elsewhere in the same text still fires. See ADR 0002.
        modifiers = global_thematic_modifiers or DEFAULT_GLOBAL_THEMATIC_MODIFIERS
        self._thematic_global_re = re.compile(
            r"\b(global|worldwide)(?=\s+(?:"
            + "|".join(re.escape(w) for w in sorted(modifiers))
            + r")\b)",
            re.IGNORECASE,
        )
        self._parsed = df["raw_mcf"].apply(self._parse)
        self._extractors = {
            "access_constraints": self._get_access_constraints,
            "rights": self._get_rights,
            "license": self._get_license,
            "opendata_keyword": self._has_opendata_keyword,
            "soil_related": self._is_soil_related,
        }

    def _parse(self, val):
        try:
            return json.loads(val)
        except (json.JSONDecodeError, TypeError):
            return None

    def _get_access_constraints(self, mcf):
        if not mcf:
            return None
        try:
            return mcf["identification"].get("accessconstraints")
        except (TypeError, KeyError):
            return None

    def _get_rights(self, mcf):
        if not mcf:
            return None
        try:
            return mcf["identification"].get("rights")
        except (TypeError, KeyError):
            return None

    def _get_license(self, mcf):
        if not mcf:
            return None
        try:
            license_obj = mcf["identification"].get("license")
            if isinstance(license_obj, dict):
                return license_obj.get("name")
            return license_obj
        except (TypeError, KeyError):
            return None

    def _has_opendata_keyword(self, mcf):
        if not mcf:
            return False
        try:
            kw_section = mcf["identification"].get("keywords", {})
            for _, group in kw_section.items():
                for kw in group.get("keywords", []):
                    if not isinstance(kw, str):
                        continue
                    norm = kw.lower().strip().replace("-", " ").replace("_", " ")
                    norm = " ".join(norm.split())
                    if "opendata" in norm or "open data" in norm:
                        return True
            return False
        except (TypeError, KeyError):
            return False

    def _contains_soil_term(self, text):
        lower = text.lower()
        return any(term in lower for term in self._soil_terms)

    def _check_mcf_keywords(self, mcf, contains_fn):
        if not mcf:
            return False
        try:
            kw_section = mcf.get("identification", {}).get("keywords", {})
            for _, group in kw_section.items():
                for kw in group.get("keywords", []):
                    if isinstance(kw, str) and contains_fn(kw):
                        return True
            return False
        except (TypeError, KeyError):
            return False

    def _is_soil_related(self, mcf):
        return self._check_mcf_keywords(mcf, contains_fn=self._contains_soil_term)

    def _find_countries_in_text(self, text):
        """Return the set of canonical EU country names found in *text*.

        Aliases are matched first (so ``"deutschland"`` resolves to
        ``"germany"``), then canonical names.

        Tokens are wrapped in ``\\b`` word boundaries only where they
        start/end in a word character, so the dotted alias ``"u.k."`` still
        fires in prose (``\\bu\\.k\\.`` with no trailing ``\\b``) while
        ``"uk"`` stays bounded. ``\\b`` (rather than the letter-boundary of
        :meth:`_find_terms_in_text`) also keeps a country followed by a
        superscript reference marker (``"Austria¹⁰"`` in author affiliation
        lists) from registering as a geographic mention.
        """
        if not isinstance(text, str):
            return set()
        lower = text.lower()
        found = set()
        for alias, country in self._eu_country_aliases.items():
            if re.search(self._wordbounded(alias), lower):
                found.add(country)
        for country in self._eu_countries:
            if country not in found:
                if re.search(self._wordbounded(country), lower):
                    found.add(country)
        return found

    @staticmethod
    def _wordbounded(token):
        left = r"\b" if re.match(r"\w", token[:1]) else ""
        right = r"\b" if re.match(r"\w", token[-1:]) else ""
        return left + re.escape(token) + right

    def _find_terms_in_text(self, text, terms):
        """Letter-boundary scan: return the list of *terms* present in *text*.

        Letter boundaries (``(?<![a-z])`` / ``(?![a-z])``) so ``"europe"`` does
        not fire inside ``"european"``. Order follows *terms* iteration.
        """
        if not isinstance(text, str):
            return []
        lower = text.lower()
        found = []
        for term in terms:
            if re.search(r'(?<![a-z])' + re.escape(term) + r'(?![a-z])', lower):
                found.append(term)
        return found

    def _get_eu_from_mcf(self, mcf):
        """Extract EU evidence from MCF keywords, tagged by provenance band.

        Each keyword *group* is routed to a band (see ADR 0002), applied in
        order (first match wins):

        - ``scope_tag`` — vocabulary == "Spatial scope" (the INSPIRE codelist).
          Checked first because it is often mis-typed ``theme``. Its values are
          scope granularities, split into ``for`` (``european``/``global``) and
          ``against`` (everything else: ``national``/``regional``/…).
        - ``place_keyword`` — ``keywords_type == "place"`` **or** a spatial
          vocabulary name (``Country``, ``Region``, ``Continents, …``). Each
          value routes to ``concrete_multi_country_terms`` /
          ``broad_eu_terms`` / ``eu_countries``.
        - non-spatial groups (theme/untyped + GEMET/AGROVOC) are dropped.

        Returns ``{"scope_tag": {...}, "place_keyword": {...}}`` or ``None``.
        """
        if not mcf:
            return None
        scope_for = []
        scope_against = []
        pk_concrete = []
        pk_broad = []
        pk_countries = set()
        try:
            kw_section = mcf.get("identification", {}).get("keywords", {})
            for _, group in kw_section.items():
                if not isinstance(group, dict):
                    continue
                kt = (group.get("keywords_type") or "").strip().lower()
                vocab = group.get("vocabulary")
                vname = ""
                if isinstance(vocab, dict):
                    vname = (vocab.get("name") or "").strip().lower()
                if vname == self._scope_tag_vocabulary:
                    for kw in group.get("keywords", []):
                        if not isinstance(kw, str) or not kw.strip():
                            continue
                        val = kw.strip().lower()
                        if val in self._scope_tag_for_values:
                            if val not in scope_for:
                                scope_for.append(val)
                        elif val not in scope_against:
                            scope_against.append(val)
                elif (kt == "place") or (vname in self._spatial_keyword_vocabularies):
                    for kw in group.get("keywords", []):
                        if not isinstance(kw, str) or not kw.strip():
                            continue
                        norm = kw.strip().lower()
                        if norm in self._concrete_multi_country_eu_terms:
                            if norm not in pk_concrete:
                                pk_concrete.append(norm)
                        elif norm in self._broad_eu_terms:
                            if norm not in pk_broad:
                                pk_broad.append(norm)
                        else:
                            country = self._eu_country_aliases.get(norm, norm)
                            if country in self._eu_countries:
                                pk_countries.add(country)
                # else: non-spatial group -> dropped
        except (TypeError, KeyError):
            pass
        result = {}
        if scope_for or scope_against:
            st = {}
            if scope_for:
                st["for"] = scope_for
            if scope_against:
                st["against"] = scope_against
            result["scope_tag"] = st
        if pk_concrete or pk_broad or pk_countries:
            pk = {}
            if pk_concrete:
                pk["concrete_multi_country_terms"] = pk_concrete
            if pk_broad:
                pk["broad_eu_terms"] = pk_broad
            if pk_countries:
                pk["eu_countries"] = sorted(pk_countries)
            result["place_keyword"] = pk
        return result or None

    def _scan_free_text(self, text):
        """Scan one free-text field (title or abstract) into the 3 buckets.

        Concrete terms and broad terms match with letter boundaries; country
        names with word boundaries (canonicalized). Returns
        ``{"concrete_multi_country_terms": …, "broad_eu_terms": …,
        "eu_countries": …}`` or ``None``. The provenance band (title vs
        abstract) is assigned by the caller; this method only does the matching.

        ``global``/``worldwide`` in a thematic phrase (``"global warming"``) is
        masked first (ADR 0002), so it does not register as a broad scope term;
        a genuine ``global`` elsewhere in the same text still fires.
        """
        if not isinstance(text, str) or not text.strip():
            return None
        text = self._thematic_global_re.sub(r"x\1", text)
        concrete = []
        broad = []
        countries = set()
        for t in self._find_terms_in_text(text, self._concrete_multi_country_eu_terms):
            if t not in concrete:
                concrete.append(t)
        for t in self._find_terms_in_text(text, self._broad_eu_terms):
            if t not in broad:
                broad.append(t)
        countries |= self._find_countries_in_text(text)
        if not concrete and not broad and not countries:
            return None
        result = {}
        if concrete:
            result["concrete_multi_country_terms"] = concrete
        if broad:
            result["broad_eu_terms"] = broad
        if countries:
            result["eu_countries"] = sorted(countries)
        return result

    def _extract_eu_related(self, col):
        mcf_results = self._parsed.apply(self._get_eu_from_mcf)
        title_results = self._df["title"].apply(self._scan_free_text)
        abstract_results = self._df["abstract"].apply(self._scan_free_text)

        def assemble(mcf_dict, title_dict, abstract_dict):
            result = {}
            if mcf_dict:
                result.update(mcf_dict)  # scope_tag, place_keyword
            if title_dict:
                result["title"] = title_dict
            if abstract_dict:
                result["abstract"] = abstract_dict
            return result or None

        self._df[col] = [
            assemble(m, t, a)
            for m, t, a in zip(mcf_results, title_results, abstract_results)
        ]

    def _check_df_title_abstract(self, contains_fn):
        def check_row(row):
            for col in ("title", "abstract"):
                val = row.get(col)
                if isinstance(val, str) and contains_fn(val):
                    return True
            return False
        return self._df.apply(check_row, axis=1)

    def _route_keyword_band(self, vname, kt):
        """The band a keyword group routes to (mirrors ``_get_eu_from_mcf``).

        First match wins: ``scope_tag`` is checked before ``place_keyword``
        because the "Spatial scope" codelist is often mis-typed ``theme``.
        ``"individual"`` is not a spatial type — its spatial use is caught by
        the ``kt == "place"`` half — so it falls through to ``dropped`` here.
        """
        if vname == self._scope_tag_vocabulary:
            return "scope_tag"
        if kt == "place" or vname in self._spatial_keyword_vocabularies:
            return "place_keyword"
        return "dropped"

    def keyword_group_census(self):
        """Tally every ``raw_mcf`` keyword group by ``(vocabulary, type)``.

        Reproduces the empirical evidence behind ADR 0002 § "The raw_mcf keyword
        structure": each keyword *group* carries a ``keywords_type``, a
        ``vocabulary.name``, and a ``keywords`` value list, and the vocabulary
        name is what tells us *what kind of thing* the values are. This method
        walks every group across the dump, normalises the routing keys
        (lowercased + stripped, exactly as ``_get_eu_from_mcf`` sees them), and
        labels each ``(vocabulary, type)`` combination with the band it routes
        to — ``scope_tag`` / ``place_keyword`` / ``dropped``.

        Returns a :class:`pandas.DataFrame`, one row per distinct
        ``(vocabulary, keywords_type)`` and sorted by group count (descending),
        with columns ``vocabulary``, ``keywords_type``, ``group_count``,
        ``value_count``, ``routed_band``, ``example_keywords`` (up to 5 distinct
        sample values). ``vocabulary`` / ``keywords_type`` are ``""`` when the
        group omits them.
        """
        tally = {}
        for mcf in self._parsed:
            if not isinstance(mcf, dict):
                continue
            try:
                kw_section = mcf.get("identification", {}).get("keywords", {})
            except (TypeError, KeyError):
                continue
            for _, group in kw_section.items():
                if not isinstance(group, dict):
                    continue
                kt = (group.get("keywords_type") or "").strip().lower()
                vocab = group.get("vocabulary")
                vname = ""
                if isinstance(vocab, dict):
                    vname = (vocab.get("name") or "").strip().lower()
                band = self._route_keyword_band(vname, kt)
                values = [kw for kw in group.get("keywords", [])
                          if isinstance(kw, str) and kw.strip()]
                cell = tally.setdefault(
                    (vname, kt),
                    {"group_count": 0, "value_count": 0,
                     "routed_band": band, "examples": []})
                cell["group_count"] += 1
                cell["value_count"] += len(values)
                for v in values:
                    if v not in cell["examples"] and len(cell["examples"]) < 5:
                        cell["examples"].append(v)
        rows = [{
            "vocabulary": vname,
            "keywords_type": kt,
            "group_count": cell["group_count"],
            "value_count": cell["value_count"],
            "routed_band": cell["routed_band"],
            "example_keywords": ", ".join(cell["examples"]),
        } for (vname, kt), cell in tally.items()]
        df = pd.DataFrame(rows, columns=[
            "vocabulary", "keywords_type", "group_count", "value_count",
            "routed_band", "example_keywords"])
        return df.sort_values("group_count", ascending=False).reset_index(drop=True)

    def extract(self, *fields):
        for field in fields:
            col = self._COLUMN_NAMES.get(field)
            if col is None:
                print(f"Unknown field: {field}")
                continue
            if field == "eu_related":
                self._extract_eu_related(col)
                print(self._df[col].apply(lambda x: x is not None).value_counts())
                print("__________________________")
                continue
            if field not in self._extractors:
                print(f"Unknown field: {field}")
                continue
            if field in self._TERM_BASED_FIELDS:
                contains_fn = getattr(self, self._TERM_BASED_FIELDS[field])
                primary = self._check_df_title_abstract(contains_fn)
                fallback = self._parsed.apply(self._extractors[field])
                self._df[col] = primary | fallback
            else:
                self._df[col] = self._parsed.apply(self._extractors[field])
            print(self._df[col].value_counts(dropna=False))
            print("__________________________")
