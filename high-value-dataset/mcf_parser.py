import json
import re


class McfParser:

    _COLUMN_NAMES = {
        "access_constraints": "access_constraints_mcf",
        "rights": "rights_mcf",
        "license": "license_mcf",
        "opendata_keyword": "has_opendata_keyword_mcf",
        "soil_related": "is_soil_related_mcf",
        "eu_related": "eu_related_terms",
        "resolution": "resolution_mcf",
        "datatype": "datatype_mcf",
    }

    DEFAULT_SOIL_TERMS = (
        "soil",
    )

    DEFAULT_EU_TERMS = (
        "eu",
        "europ" #subsring matching will include europe, europa, european (union), ...
    )

    DEFAULT_MULTI_COUNTRY_EU_TERMS = frozenset({
        "eea39",
        "eea38 (from 2020)",
        "eu28 (2013-2020)",
        "eu27 (2007-2013)",
        "eu27 (from 2020)",
        "eu27",
        "eu25",
        "eu15",
        "europe",
        "europa",
        "european union",
        "efta4",
    })

    DEFAULT_EU_COUNTRIES = frozenset({
        "austria", "belgium", "bulgaria", "croatia", "cyprus",
        "czechia", "denmark", "estonia", "finland", "france",
        "germany", "greece", "hungary", "ireland", "italy",
        "latvia", "lithuania", "luxembourg", "malta", "netherlands",
        "poland", "portugal", "romania", "slovakia", "slovenia",
        "spain", "sweden",
    })

    DEFAULT_EU_COUNTRY_ALIASES = {
        "bundesrepublik deutschland": "germany",
        "deutschland": "germany",
        "czech republic": "czechia",
        "españa": "spain",
        "finnland": "finland",
        "italia": "italy",
        "nederland": "netherlands",
        "suomi": "finland",
        "the netherlands": "netherlands",
    }

    _TERM_BASED_FIELDS = {
        "soil_related": "_contains_soil_term",
    }

    def __init__(self, df, soil_terms=None, eu_terms=None,
                 multi_country_eu_terms=None, eu_countries=None,
                 eu_country_aliases=None):
        self._df = df
        self._soil_terms = soil_terms or self.DEFAULT_SOIL_TERMS
        self._eu_terms = eu_terms or self.DEFAULT_EU_TERMS
        self._multi_country_eu_terms = (
            multi_country_eu_terms or self.DEFAULT_MULTI_COUNTRY_EU_TERMS
        )
        self._eu_countries = eu_countries or self.DEFAULT_EU_COUNTRIES
        self._eu_country_aliases = (
            eu_country_aliases or self.DEFAULT_EU_COUNTRY_ALIASES
        )
        self._parsed = df["raw_mcf"].apply(self._parse)
        self._extractors = {
            "access_constraints": self._get_access_constraints,
            "rights": self._get_rights,
            "license": self._get_license,
            "opendata_keyword": self._has_opendata_keyword,
            "soil_related": self._is_soil_related,
            "resolution": self._get_resolution,
            "datatype": self._get_datatype,
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

    def _contains_eu_term(self, text):
        lower = text.lower()
        for term in self._eu_terms:
            if term == "eu":
                if re.search(r"\beu\b", lower):
                    return True
            elif term in lower:
                return True
        return False

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

    def _find_eu_terms_in_text(self, text):
        if not isinstance(text, str):
            return []
        lower = text.lower()
        found = []
        for term in self._eu_terms:
            if term == "eu":
                if re.search(pattern=r"\beu\b", string=lower):
                    found.append("eu")
            elif term in lower:
                found.append(term)
        return found

    def _find_places_in_text(self, text):
        if not isinstance(text, str):
            return []
        lower = text.lower()
        found = []
        seen_countries = set()
        for alias, country in self._eu_country_aliases.items():
            if re.search(r'\b' + re.escape(alias) + r'\b', lower):
                if country not in seen_countries:
                    found.append(alias)
                    seen_countries.add(country)
        for country in self._eu_countries:
            if country not in seen_countries:
                if re.search(r'\b' + re.escape(country) + r'\b', lower):
                    found.append(country)
                    seen_countries.add(country)
        return found

    def _find_multi_country_terms_in_text(self, text):
        if not isinstance(text, str):
            return []
        lower = text.lower()
        found = []
        for term in self._multi_country_eu_terms:
            if term in lower:
                found.append(term)
        return found

    def _get_eu_from_mcf(self, mcf):
        if not mcf:
            return None
        mcf_eu_keywords = []
        place_keywords = []
        try:
            kw_section = mcf.get("identification", {}).get("keywords", {})
            for _, group in kw_section.items():
                for kw in group.get("keywords", []):
                    if not isinstance(kw, str) or not kw.strip():
                        continue
                    norm = kw.strip().lower()
                    if self._contains_eu_term(kw):
                        if kw.strip() not in mcf_eu_keywords:
                            mcf_eu_keywords.append(kw.strip())
                    if norm in self._multi_country_eu_terms:
                        if kw.strip() not in place_keywords:
                            place_keywords.append(kw.strip())
                    else:
                        country = self._eu_country_aliases.get(norm, norm)
                        if country in self._eu_countries:
                            if kw.strip() not in place_keywords:
                                place_keywords.append(kw.strip())
        except (TypeError, KeyError):
            pass
        if not mcf_eu_keywords and not place_keywords:
            return None
        result = {}
        if mcf_eu_keywords:
            result["mcf_eu_keywords"] = mcf_eu_keywords
        if place_keywords:
            result["place_keywords"] = place_keywords
        return result

    def _get_eu_from_title_abstract_row(self, row):
        eu_terms = []
        places = []
        for col in ("title", "abstract"):
            val = row.get(col)
            if not isinstance(val, str) or not val.strip():
                continue
            for t in self._find_eu_terms_in_text(val):
                if t not in eu_terms:
                    eu_terms.append(t)
            for p in self._find_places_in_text(val):
                if p not in places:
                    places.append(p)
            for t in self._find_multi_country_terms_in_text(val):
                if t not in places:
                    places.append(t)
        if not eu_terms and not places:
            return None
        result = {}
        if eu_terms:
            result["title_abstract_eu"] = eu_terms
        if places:
            result["title_abstract_places"] = places
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

    def _get_resolution(self, mcf):
        if not mcf:
            return None
        try:
            resolutions = mcf.get("spatial", {}).get("resolution", [])
            if resolutions:
                r = resolutions[0]
                distance = r.get("distance", "")
                uom = r.get("uom", "")
                return f"{distance} {uom}".strip() if distance else None
            return None
        except (TypeError, KeyError, IndexError):
            return None

    def _get_datatype(self, mcf):
        if not mcf:
            return None
        try:
            dt = mcf.get("spatial", {}).get("datatype")
            return dt if dt else None
        except (TypeError, KeyError):
            return None

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


def classify_multi_country_eu(df, eu_related_terms_col="eu_related_terms",
                               spatial_desc_col="spatial_desc",
                               eu_countries=None,
                               eu_country_aliases=None,
                               multi_country_eu_terms=None):
    eu_countries = eu_countries or McfParser.DEFAULT_EU_COUNTRIES
    eu_country_aliases = eu_country_aliases or McfParser.DEFAULT_EU_COUNTRY_ALIASES
    multi_country_eu_terms = (
        multi_country_eu_terms or McfParser.DEFAULT_MULTI_COUNTRY_EU_TERMS
    )
    BROAD_TERMS = frozenset({"worldwide", "global"})

    def parse_spatial_desc(desc):
        if not isinstance(desc, str) or not desc.strip():
            return None
        lower = desc.strip().lower()
        if lower in BROAD_TERMS:
            return "broad"
        if lower in {"other"}:
            return None
        if lower in multi_country_eu_terms:
            return "multi_country"
        parts = [p.strip() for p in lower.split(",")]
        has_multi_country = False
        eu_count = 0
        for part in parts:
            if not part:
                continue
            if part in BROAD_TERMS:
                return "broad"
            if part in multi_country_eu_terms:
                has_multi_country = True
            else:
                country = eu_country_aliases.get(part, part)
                if country in eu_countries:
                    eu_count += 1
        if has_multi_country or eu_count >= 2:
            return "multi_country"
        if eu_count == 1:
            return "single_country"
        return "non_eu"

    def get_geo_evidence(terms):
        if terms is None:
            return "none"
        all_raw = (
            terms.get("place_keywords", [])
            + terms.get("title_abstract_places", [])
        )
        if not all_raw:
            return "no_countries"
        has_multi_country = False
        eu_countries_found = set()
        for p in all_raw:
            norm = p.lower().strip()
            if norm in multi_country_eu_terms:
                has_multi_country = True
            else:
                canonical = eu_country_aliases.get(norm, norm)
                if canonical in eu_countries:
                    eu_countries_found.add(canonical)
        if has_multi_country:
            return "multi_country"
        if len(eu_countries_found) >= 2:
            return "multi_country"
        if len(eu_countries_found) == 1:
            return "single_country"
        return "no_countries"

    def has_vague_eu_terms(terms):
        if terms is None:
            return False
        return bool(terms.get("mcf_eu_keywords")) or bool(
            terms.get("title_abstract_eu")
        )

    def classify_row(row):
        terms = row[eu_related_terms_col]

        if terms is None:
            default = False
        else:
            geo = get_geo_evidence(terms)
            if geo == "multi_country":
                default = True
            elif geo == "single_country":
                default = False
            else:
                default = has_vague_eu_terms(terms)

        spatial = parse_spatial_desc(row[spatial_desc_col])

        if spatial is None:
            return default

        if default and spatial == "single_country":
            return False
        if default and spatial == "non_eu":
            return False
        if not default and spatial == "multi_country":
            return True
        if not default and spatial == "broad":
            return True

        return default

    return df.apply(classify_row, axis=1)
