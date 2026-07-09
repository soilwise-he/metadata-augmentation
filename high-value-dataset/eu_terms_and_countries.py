"""European country sets, name aliases, and scope-term lexicons.

The single source of truth for what counts as European, in two parts: the
canonical country set plus the alias maps that normalize the names the GeoJSON
and authors actually use; and the scope-term lexicons (concrete acronyms, broad
terms, provenance codelists) the text classifiers match against free text.

Consumed by :mod:`bbox_classifier` (country set + aliases),
:mod:`mcf_parser` (aliases + lexicons), and :mod:`spatial_coverage`.

"European" means the continent, not the political Union — non-EU-member states
(Norway, Switzerland, the U.K., ...) are included.
"""

# The European continent (45 states): every sovereign European state in the
# source GeoJSON plus Turkey and Cyprus (classified "Asia" in the geojson but European).
# Canonical lowercase short English names.
DEFAULT_EU_COUNTRIES = frozenset({
    "albania", "andorra", "austria", "belarus", "belgium",
    "bosnia and herzegovina", "bulgaria", "croatia", "cyprus",
    "czechia", "denmark", "estonia", "finland", "france",
    "germany", "greece", "hungary", "iceland", "ireland",
    "italy", "latvia", "liechtenstein", "lithuania", "luxembourg",
    "macedonia", "malta", "moldova", "monaco", "montenegro",
    "netherlands", "norway", "poland", "portugal", "romania",
    "russia", "san marino", "serbia", "slovakia", "slovenia",
    "spain", "sweden", "switzerland", "turkey", "ukraine",
    "united kingdom",
})

# GeoJSON long-form name → canonical. 45 entries; most are trivial lowercasing,
# six are genuine rewrites (the GeoJSON carries bureaucratic forms that
# never appear in free text). bbox_classifier resolves each GeoDataFrame row
# through this map to decide EU membership. Every value is in
# DEFAULT_EU_COUNTRIES (asserted by the country-set agreement test).
DEFAULT_GEOJSON_NAME_ALIASES = {
    "Albania": "albania",
    "Andorra": "andorra",
    "Austria": "austria",
    "Belarus": "belarus",
    "Belgium": "belgium",
    "Bosnia & Herzegovina": "bosnia and herzegovina",
    "Bulgaria": "bulgaria",
    "Croatia": "croatia",
    "Cyprus": "cyprus",
    "Czech Republic": "czechia",
    "Denmark": "denmark",
    "Estonia": "estonia",
    "Finland": "finland",
    "France": "france",
    "Germany": "germany",
    "Greece": "greece",
    "Hungary": "hungary",
    "Iceland": "iceland",
    "Ireland": "ireland",
    "Italy": "italy",
    "Latvia": "latvia",
    "Liechtenstein": "liechtenstein",
    "Lithuania": "lithuania",
    "Luxembourg": "luxembourg",
    "Macedonia": "macedonia",
    "Malta": "malta",
    "Moldova, Republic of": "moldova",
    "Monaco": "monaco",
    "Montenegro": "montenegro",
    "Netherlands": "netherlands",
    "Norway": "norway",
    "Poland": "poland",
    "Portugal": "portugal",
    "Romania": "romania",
    "Russian Federation": "russia",
    "San Marino": "san marino",
    "Serbia": "serbia",
    "Slovakia": "slovakia",
    "Slovenia": "slovenia",
    "Spain": "spain",
    "Sweden": "sweden",
    "Switzerland": "switzerland",
    "The former Yugoslav Republic of Macedonia": "macedonia",
    "Turkey": "turkey",
    "U.K. of Great Britain and Northern Ireland": "united kingdom",
    "Ukraine": "ukraine",
}

# Free-text / metadata variant → canonical. Used by mcf_parser.py and
# spatial_coverage.py to match what authors actually write to our default EU names. 
# Keys are lowercased on matching, so keep them lowercase.
DEFAULT_EU_COUNTRY_ALIASES = {
    "bundesrepublik deutschland": "germany",
    "deutschland": "germany",
    "czech republic": "czechia",
    "cesko": "czechia",
    "españa": "spain",
    "espana": "spain",
    "spanien": "spain",
    "finnland": "finland",
    "suomi": "finland",
    "italia": "italy",
    "italien": "italy",
    "nederland": "netherlands",
    "the netherlands": "netherlands",
    "russian federation": "russia",
    "russie": "russia",
    "uk": "united kingdom",
    "u.k.": "united kingdom",
    "great britain": "united kingdom",
    "britain": "united kingdom",
    "macedonia": "macedonia",
    "north macedonia": "macedonia",
    "fyrom": "macedonia",
    "bosnia": "bosnia and herzegovina",
    "republic of moldova": "moldova",
    "hellas": "greece",
    "hellenic republic": "greece",
}

# Concrete multi-country scope acronyms — labels that *enumerate* a defined
# European geography (EEA39 = the 39 EEA countries, EU27 = the 27 members, …).
#
# Bare, spaced, and parenthesized forms coexist because mcf_parser matches
# keywords by EXACT set-membership but titles/abstracts by boundary regex: the
# parenthesized form is the only one that appears as a literal keyword value,
# while the bare acronym fires only in prose — so none is redundant.
DEFAULT_CONCRETE_MULTI_COUNTRY_EU_TERMS = frozenset({
    "eea39", "eea 39",
    "eea38", "eea 38", "eea38 (from 2020)",
    "eu28", "eu 28", "eu28 (2013-2020)",
    "eu27", "eu 27", "eu27 (2007-2013)", "eu27 (from 2020)",
    "eu25",
    "eu15",
    "efta4",
    "pan-eu", "pan eu",
    "eu-wide", "eu wide",
})

# Broad EU terms — labels that signal European-ness *without enumerating* a
# geography. Ambiguous between a spatial scope ("soils … in Europe") and a
# thematic mention ("European Commission"); 
DEFAULT_BROAD_EU_TERMS = frozenset({
    "europe",
    "europa",
    "european union",
    "european",
    "global",
    "worldwide",
})

# --- provenance reference sets for keyword extraction from "raw_mcf" (see ADR 0002) --------------------------------

# The vocabulary name that identifies the scope_tag band — the INSPIRE "Spatial
# scope" codelist. Checked FIRST when classifying a keyword group, because it is
# often mis-typed `theme`.
DEFAULT_SCOPE_TAG_VOCABULARY = "spatial scope"

# Vocabulary names that mark a keyword group as geographic (place_keyword band)
# regardless of the often-missing keywords_type. "individual" is deliberately
# excluded: under non-place types it carries thematic keywords ("opendata",
# "Intensive forest monitoring"); its spatial use is already caught by the
# keywords_type == "place" rule.
DEFAULT_SPATIAL_KEYWORD_VOCABULARIES = frozenset({
    "country",
    "region",
    "continents, countries, sea regions of the world.",
    "continents, countries and sea regions of the world.",
})

# Values of the "Spatial scope" codelist that mean multi-country scope -> S3 for.
# Every other value (national / regional / local / project + their translations)
# means sub-multi-country scope -> S2 against. The codelist cannot carry country
# names or EEA-style acronyms (verified: 0 in 1,493 values).
DEFAULT_SCOPE_TAG_FOR_VALUES = frozenset({
    "european",
    "global",
})

# Nouns that, when they immediately follow ``global``/``worldwide`` in free text,
# mark that occurrence as a *thematic* modifier rather than a spatial scope —
# ``global warming``, ``global change``, ``global fire risk``, ``worldwide leaf
# economics spectrum``. ``global``/``worldwide`` in such a phrase is not a
# multi-country scope signal (see ADR 0002). Used by :mod:`mcf_parser` to mask
# these phrases before broad-term matching, so a genuine ``global`` elsewhere in
# the same title (``"Global Soil Erosion and Global Warming Potential"``) still
# fires. High-precision, deliberately narrow; ambiguous collocations
DEFAULT_GLOBAL_THEMATIC_MODIFIERS = frozenset({
    "warming", "change", "changes", "environmental", "fire", "fires",
    "radiation", "greenhouse", "methane", "nitrous",
    "biogeochemical", "biogeochemistry", "dilemma", "leaf", "leaves",
    "economy", "economic", "temperature", "temperatures",
    "dimming", "stabilization", "burned", "burning",
})
