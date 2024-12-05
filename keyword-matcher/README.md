
## Keyword Matcher

Analyses existing keywords on a metadata record, it matches an existing keyword to a list of predefined keywords, augmenting the keyword to include a thesaurus and uri reference (potentially a translation to english)

It requires a database with common thesauri

A match is made by label, or aliases (sameAs, closeMatch), may require translation.

## Development status

- Initial development as part of [harvest module](https://github.com/soilwise-he/harvesters/blob/main/utils/keyword_matching.py), a keyword is validated against a know list of keywords, if a match is available a URI of the keyword is returned

- In next iteration, the keywords can be populated from a sparql query: `select skos:concept where skos:prefLabel[@lang=='{language}']=={keyword}`, it means we should prepare a triple store with selected taxonomies (glosis, agrovoc, gemet, inspire, ...). To verify if [soilhealth knowledge graph](https://github.com/soilwise-he/soil-health-knowledge-graph) is a good location for this.

## A brief reading guide(28-11-2024):

- code:
    - ***get_thesaurus.py***: tranform soil health knowledge to a concept set (concept.json) by fetching labels from Agrovoc and iso.
    - ***match.py***: match records with the concept set

- result data:
    - ***match.json***: result of maching records with concepts, the same as the db table: harvest.match
    - ***mis_keys.json***: a list of keywords that failed to find a match from the concepts
    - ***vague_match***: matched labels with a similarity score >= 80 and <100




