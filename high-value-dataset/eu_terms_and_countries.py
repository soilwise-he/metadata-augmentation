"""Canonical European geography reference sets.

sets the classifiers match against free text. Consumed by :mod:`bbox_classifier`,
The single source of truth for which countries count as European, plus the 
of terms :mod:`mcf_parser`, and :mod:`spatial_coverage`; see those for the algorithms.

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

# Concrete multi-country scope acronyms (deliberate pan-European labels) plus
# global-scope words. A strong textual signal in spatial_coverage: sticky, bbox
# cannot override.
DEFAULT_CONCRETE_MULTI_COUNTRY_EU_TERMS = frozenset({
    "eea39",
    "eea38 (from 2020)",
    "eu28 (2013-2020)",
    "eu27 (2007-2013)",
    "eu27 (from 2020)",
    "eu27",
    "eu25",
    "eu15",
    "efta4",
    "pan-eu",
    "pan eu",
    "eu-wide",
    "eu wide",
    "worldwide",
    "global",
})

# Broad EU terms — ambiguous between a spatial scope ("soils … in Europe") and a
# thematic mention ("European Commission", "a European Union target"). A weak
# textual signal in spatial_coverage: can be overwritten by the bbox if present.
DEFAULT_BROAD_EU_TERMS = frozenset({
    "europe",
    "europa",
    "european union",
})
