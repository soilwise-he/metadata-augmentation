# Metadata augmentation

Use nlp/llm on a resource to augment metadata statements about a resource

## Translation module

Many records arrive in a local language, we aim to capture at least english main properties for the record: title, abstract, keywords, lineage, usage constraints

- has a db backend, every translation is captured in a database
- the EU translation service is used, this service returns a asynchronous response to an API endpoint (callback)
- the callback populates the database, next time the translation is available

Read more at <https://language-tools.ec.europa.eu/>

## Keyword Matcher

Analyses existing keywords on a metadata record, it matches an existing keyword to a list of predefined keywords, augmenting the keyword to include a thesaurus and uri reference (potentially a translation to english)

It requires a database (relational or rdf) with common thesauri

## Spatial Locator

Analyses existing keywords to find a relevant geography for the record, it then uses the geonames api to find spatial coordinates for the geography, which are inserted into the metadata record

