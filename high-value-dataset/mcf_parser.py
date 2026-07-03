import json
import re

from eu_terms_and_countries import (
    DEFAULT_EU_COUNTRIES,
    DEFAULT_EU_COUNTRY_ALIASES,
    DEFAULT_CONCRETE_MULTI_COUNTRY_EU_TERMS,
    DEFAULT_BROAD_EU_TERMS,
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
                 eu_country_aliases=None):
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
        """Extract EU evidence from MCF keywords into 3 pooled buckets.

        Each lowercased keyword routes to exactly one bucket:

        - ``concrete_multi_country_terms`` — a concrete pan-European acronym
          (S3), e.g. ``eea39``, ``eu27``.
        - ``broad_eu_terms`` — a broad EU term (S1): ``europe``, ``europa``,
          ``european union``.
        - ``eu_countries`` — a canonical EU country name (S2).

        Returns ``None`` when no keyword yields any evidence.
        """
        if not mcf:
            return None
        concrete = []
        broad = []
        countries = set()
        try:
            kw_section = mcf.get("identification", {}).get("keywords", {})
            for _, group in kw_section.items():
                for kw in group.get("keywords", []):
                    if not isinstance(kw, str) or not kw.strip():
                        continue
                    norm = kw.strip().lower()
                    if norm in self._concrete_multi_country_eu_terms:
                        if norm not in concrete:
                            concrete.append(norm)
                    elif norm in self._broad_eu_terms:
                        if norm not in broad:
                            broad.append(norm)
                    else:
                        country = self._eu_country_aliases.get(norm, norm)
                        if country in self._eu_countries:
                            countries.add(country)
        except (TypeError, KeyError):
            pass
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

    def _get_eu_from_title_abstract_row(self, row):
        """Extract EU evidence from title + abstract into the same 3 buckets.

        Free text is scanned for concrete multi-country terms (letter
        boundaries), broad EU terms (letter boundaries), and EU country names
        (word boundaries, canonicalized). Buckets are pooled across title and
        abstract. Returns ``None`` when nothing is found.
        """
        concrete = []
        broad = []
        countries = set()
        for col in ("title", "abstract"):
            val = row.get(col)
            if not isinstance(val, str) or not val.strip():
                continue
            for t in self._find_terms_in_text(val, self._concrete_multi_country_eu_terms):
                if t not in concrete:
                    concrete.append(t)
            for t in self._find_terms_in_text(val, self._broad_eu_terms):
                if t not in broad:
                    broad.append(t)
            countries |= self._find_countries_in_text(val)
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
        ta_results = self._df.apply(self._get_eu_from_title_abstract_row, axis=1)

        def merge(mcf_dict, ta_dict):
            if mcf_dict is None and ta_dict is None:
                return None
            result = {}
            if mcf_dict:
                for k, v in mcf_dict.items():
                    if v:
                        result[k] = list(v)
            if ta_dict:
                for k, v in ta_dict.items():
                    if v:
                        if k in result:
                            for item in v:
                                if item not in result[k]:
                                    result[k].append(item)
                        else:
                            result[k] = list(v)
            return result if result else None

        self._df[col] = [
            merge(m, t) for m, t in zip(mcf_results, ta_results)
        ]

    def _check_df_title_abstract(self, contains_fn):
        def check_row(row):
            for col in ("title", "abstract"):
                val = row.get(col)
                if isinstance(val, str) and contains_fn(val):
                    return True
            return False
        return self._df.apply(check_row, axis=1)

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
