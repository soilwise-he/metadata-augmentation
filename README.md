# Metadata augmentation

Repository relates mainly to task 2.3

>  AI and ML for data findability and accessibility, M7-M46 (ISRIC, EV ILVO, WU, CREA, WE)
> 
>  AI and ML techniques will be used to [analyze (meta)data gaps](https://github.com/soilwise-he/metadata-augmentation/issues/9) and to [complete/update metadata from available resources](https://github.com/soilwise-he/metadata-augmentation/issues/10) 
>  to [automatize the metadata adoption process](https://github.com/soilwise-he/metadata-augmentation/issues/11) and limit the amount of human effort. SIEUSOIL/FAO and INSPIRE 
>  [ontologies will be used as a basis for semantics-related tasks](https://github.com/soilwise-he/metadata-augmentation/issues/13). AI- and ML-based linking and indexing based on 
>  [thesauri and gazetteers](https://github.com/soilwise-he/metadata-augmentation/issues/12) will enhance the state-of-the-art cataloguing and findability tools to support stakeholders in 
>  obtaining the most relevant results. As such, we will provide [precise and personalized answers](https://github.com/soilwise-he/metadata-augmentation/issues/14) that [users can act 
>  on immediately](https://github.com/soilwise-he/metadata-augmentation/issues/15). AI and ML semantic inference will also be used to [check the persistency and consistency of data 
>  asset identification](https://github.com/soilwise-he/metadata-augmentation/issues/16) across multiple resources. [Outcome: D3.1](docs/D3.1/index.md)


## Storage

Augmentations are stored on a dedicated augmentation table, indicating the process which produced it.

On the database we have 2 tables related to augmentation

```SQL
    CREATE TABLE IF NOT EXISTS metadata.augments
    (
    record_id text,
    property text,
    value text,
    process text,
    date timestamp with time zone DEFAULT now()
    );

    CREATE TABLE IF NOT EXISTS metadata.augment_status
    (
    record_id text,
    status text,
    process text,
    date timestamp with time zone DEFAULT now()
    );
```

An augment process is typically organised as: 

- Each augmenter will run a query against harvest.items joined to augment_status to see if there are records to be processed.
- processes a limited set (100) and continue with the next set via task-scheduler
- augment results are written to metadata.augments, please specify:
    - the record_id processed
    - the property which is improved (for example: title, abstract, keywords, license)
    - the value which has been calculated
    - the process which produced the value (for example NER-augmenation, spatial locator)
- Finally the augment_stutus is updated to indicate that the record has been processed

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

## Element matcher

Matches elements such as license, type using a similar approach as keyword matcher

## Keyword extracter

Use NLP/LLM to extract relevant keywords from title/abstract/content

## Spatial Locator

Analyses existing keywords to find a relevant geography for the record, it then uses the geonames api to find spatial coordinates for the geography, which are inserted into the metadata record

[read more](./spatial-locator/)

## Spatial scope analyser

[read more](./spatial-scope-analyser/)

## DOI enricher

This script identifies records identified by a DOI, DOI metadata is extracted from OpenAire or Datacite to enrich the record.

## Youtube

This script identifies records refering to a youtube video or youtube playlist. If so, metadata of the video is ingested from the youtube platform.

[read more](./youtube/)

## GDAL metadata

For those records which refer to a spatial file or spatial data service, the file or service is analysed for technical details such as format, projection, geometry type, bounding box. The record is enriched with this information.

## Schema.org enricher

For those records which refer to a website, the website is analysed to understand if it contains schema.org or open graph metadata.

## Zenodo enricher

Zenodo is an important repository for Horizon Europe. Zenodo captures some metadata elements which are not propagated by OpenAire. If a record refers to zenodo, these additional elements are captured from a dedicated Zenodo API.