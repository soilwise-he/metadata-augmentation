# Metadata augmentation

This component include a number of module which each apply a certain methodology to augment metadata records to better fit the discoverability, accessibility and reusability of the resource.

Augmentations are stored on a dedicated augmentation table, indicating the process which produced it.

| metadata-uri | metadata-element | source | value | proces | date |
| --- | --- | --- | --- | --- | --- |
| https://geo.fi/data/ee44-aa22-33 | spatial-scope | 16.7,62.2,18,81.5 |  https://inspire.ec.europa.eu/metadata-codelist/SpatialScope/national | spatial-scope-analyser | 2024-07-04 |
| https://geo.fi/data/abc1-ba27-67 | soil-thread | This dataset is used to evaluate Soil Compaction in Nuohous Sundström | http://aims.fao.org/aos/agrovoc/c_7163 | keyword-analyser | 2024-06-28 |

Scripts in this repo extract a subset of records for which no augmentation has been predicted yet, or an existing augmentation should be updated (and the source record does not already provide the missing content). This can be achieved by querying the records table with a left-join to the augmentation table (or an inner query). Database connection parameters are inserted via environment variables.

Augmentation tasks can best be triggered from a task runner, such as a CI-CD pipeline. Configuration scripts for running various tasks in a Gitlab CI-CD environment are available in [CI](./CI/). Tasks are configured using environment variables. 

The following modules are available:

## Translation module

Many records arrive in a local language, we aim to capture at least english main properties for the record: title, abstract, keywords, lineage, usage constraints

- has a db backend, every translation is captured in a database
- the EU translation service is used, this service returns a asynchronous response to an API endpoint (callback)
- the callback populates the database, next time the translation is available

Read more at <https://language-tools.ec.europa.eu/>

[Read more](./translation/)

## Keyword Matcher

Analyses existing keywords on a metadata record, it matches an existing keyword to a list of predefined keywords, augmenting the keyword to include a thesaurus and uri reference (potentially a translation to english)

It requires a database (relational or rdf) with common thesauri

[Read more](./keyword-matcher/)

## Element matcher

Applies a similar mechanism as the keyword matcher, but to any configured metadata element. It clusters the content of an element to a set of given values, based on configured synonyms and translations. Unmatched terms are kept aside in order to update the configuration for later iterations.

[Read more](./element-matcher)

## Spatial Locator

Analyses existing keywords to find a relevant geography for the record, it then uses the geonames api to find spatial coordinates for the geography, which are inserted into the metadata record

[Read more](./spatial-locator/)

## Spatial scope analyser

An algorythm to understand if the spatial scope mentioned in a record, matches with a local, regional, national or continental scope.

[Read more](./spatial-scope-analyser/)

---

## SoilWise-he project

This work has been initiated as part of the [Soilwise-he project](https://soilwise-he.eu). 
The project receives funding from the European Union’s HORIZON Innovation Actions 2022 under grant agreement No. 101112838.

The work relates to task 2.3 of the project

> AI and ML for data findability and accessibility 
> Partners involved are: ISRIC, EV ILVO, WU, CREA, WE
> 
>  AI and ML techniques will be used to [analyze (meta)data gaps](https://github.com/soilwise-he/metadata-augmentation/issues/9) and to [complete/update metadata from available resources](https://github.com/soilwise-he/metadata-augmentation/issues/10) 
>  to [automatize the metadata adoption process](https://github.com/soilwise-he/metadata-augmentation/issues/11) and limit the amount of human effort. SIEUSOIL/FAO and INSPIRE 
>  [ontologies will be used as a basis for semantics-related tasks](https://github.com/soilwise-he/metadata-augmentation/issues/13). AI- and ML-based linking and indexing based on 
>  [thesauri and gazetteers](https://github.com/soilwise-he/metadata-augmentation/issues/12) will enhance the state-of-the-art cataloguing and findability tools to support stakeholders in 
>  obtaining the most relevant results. As such, we will provide [precise and personalized answers](https://github.com/soilwise-he/metadata-augmentation/issues/14) that [users can act 
>  on immediately](https://github.com/soilwise-he/metadata-augmentation/issues/15). AI and ML semantic inference will also be used to [check the persistency and consistency of data 
>  asset identification](https://github.com/soilwise-he/metadata-augmentation/issues/16) across multiple resources. [Outcome: D3.1](docs/D3.1/index.md)
