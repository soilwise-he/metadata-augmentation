
## Keyword Matcher

Analyses existing keywords on a metadata record, it matches an existing keyword to a list of predefined keywords, augmenting the keyword to include a thesaurus and uri reference (potentially a translation to english)

It requires a database (relational or rdf) with common thesauri

## Ideas / Questions

- at this point i assume a sparql query: `select skos:concept where skos:prefLabel[@lang=='{language}']=={keyword}`, it means we should prepare a triple store with selected taxonomies (glosis, agrovoc, gemet, inspire, ...)
- glosis is currently not multilingual yet (translate first?)



