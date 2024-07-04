# Metadata augmentation

Use scripting/nlp/llm on a resource to augment metadata statements about a resource.

Augmentations are stored on a dedicated augmentation table, indicating the process which produced it.

| metadata-uri | metadata-element | source | value | proces | date |
| --- | --- | --- | --- | --- | --- |
| https://geo.fi/data/ee44-aa22-33 | spatial-scope | 16.7,62.2,18,81.5 |  https://inspire.ec.europa.eu/metadata-codelist/SpatialScope/national | spatial-scope-analyser | 2024-07-04 |
| https://geo.fi/data/abc1-ba27-67 | soil-thread | This dataset is used to evaluate Soil Compaction in Nuohous Sundström | http://aims.fao.org/aos/agrovoc/c_7163 | keyword-analyser | 2024-06-28 |

Scripts in this repo extract a subset of records for which no augmentation has been predicted yet, or an existing augmentation should be updated (and the source record does not already provide the missing content). This can be achieved by querying the records table with a left-join to the augmentation table (or an inner query). Database connection parameters are inserted via environment variables.

At intervals the code is released as a docker image, which can be used in CI-CD scripts.

## Translation module

Many records arrive in a local language, we aim to capture at least english main properties for the record: title, abstract, keywords, lineage, usage constraints

- has a db backend, every translation is captured in a database
- the EU translation service is used, this service returns a asynchronous response to an API endpoint (callback)
- the callback populates the database, next time the translation is available

Read more at <https://language-tools.ec.europa.eu/>

[read more](./translation/)

## Keyword Matcher

Analyses existing keywords on a metadata record, it matches an existing keyword to a list of predefined keywords, augmenting the keyword to include a thesaurus and uri reference (potentially a translation to english)

It requires a database (relational or rdf) with common thesauri

[read more](./keyword-matcher/)

## Spatial Locator

Analyses existing keywords to find a relevant geography for the record, it then uses the geonames api to find spatial coordinates for the geography, which are inserted into the metadata record

[read more](./spatial-locator/)

## spatial scope analyser

[read more](./spatial-scope-analyser/)