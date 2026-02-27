# Design Document: Metadata validator

## Introduction

### Component Overview and Scope

SoilWise Repository aims for the approach to harvest and register as much as possible. Catalogues which capture metadata authored by data custodians typically have a wide range of metadata completeness and accuracy. Therefore, the SoilWise Repository employs metadata validation mechanisms to provide additional information about metadata completeness, conformance and integrity. Information resulting from the validation process are stored together with each metadata record in a relational database and updated after registering a new metadata version.

It is important to understand that this activity runs on unprocessed records at entry to the system. Which means, since there is a n:1 relation between ingested records and deduplicated, enhanced records in the Soilwise repository, a validation result should always be evaluated by its combination of record identifier and source and moment of harvesting. Validation results can best be visualised on record level, by showing a historic list of sources which contributed to the record.

### Users

1. **authorised data users**
    - see validation results (log) per each metadata record
2. **unauthorised data users**
3. **administrators**
    - manually run validation
    - see summary validation results
    - monitor validation process

### References

- [INSPIRE validator](https://inspire.ec.europa.eu/validator/home/index.html)
- [pygeometa](https://geopython.github.io/pygeometa/tutorial/#for-users)
- [Hale Connect](https://help.wetransform.to/docs/tutorials/2018-04-30-metadata-tutorial/)

## Requirements

### Functional Requirements

- regular automated validation, at least once per six months after the first project year
- validation result is stored in database related to the harvested record
- metadata of datasets, documents,journal articles, ...
- metadata in ISO19139:2007, Dublin Core, ...
- information about metadata completeness (are elements populated)
- information about metadata conformance (schema, profile, ...)?
- information about metadata integrity (are elements consistent with each other)
- Link liveliness assessment

### Nice to have

- Provide means for validation of data
    - Data structure
    - Content integrity
    - Data duplication   
- Capture results of manual tests (in case not all test case can be automated)

### Non-functional Requirements

- follow ATS ETS approach and implement AI / ML enrichment
- reach TRL 7
- adopt the ISO 19157 data quality measures
- JRC does not want to discourage data providers from publishing metadata by visualizing, that they are not conformant with a set of rules

## Architecture Abstract test suites

### Technological Stack

Mardown file on Github

## Architecture Executable test suites

### Technological Stack

Harvested records are stored in 'harvest.items' table (postgres db), they are identified by their `hash`. Multiple editions of the same record are available, which each can have a different validation result.
A scheduled process (ci-cd, argo-workflows) triggers a task to understand which items have not been validated before and validates them.
results are stored on the 

### Overview of Key Features

1. [Link Liveliness Assessment](/link-liveliness-assessment/blob/main/docs/design.md): to validate if the reference link is currently working, or deprecated. This is a separate component, which returns a http status: `200 OK`, `401 Non Authorized`, `404 Not Found`, `500 Server Error` and timestamp.


### Component Diagrams

### Sequence Diagram

### Database Design

Two tables, related via record hash, because the result is stored per indicator. But main table has an overall summary.

Validation-results

| record-hash (str) | result-summary (int) | date (date) | 
| --- | --- | --- |
| UaE4GeF | 64 | 2025-01-12T11-06-34Z | 

Validation-by-indicator

| record-hash (str) | indicator (str) | result (str) | 
| --- | --- | --- |
| UaE4GeF | completeness | 77 |

### Integrations & Interfaces

- validation components run as a schedules process, it uses `harvest.items` table as a source
- [Link Liveliness Assessment](/link-liveliness-assessment/blob/main/docs/design.md) runs as a scheduled process, it uses `public.records` as a source 
- Harvester prepares the metadata in harvest.items `as is`, to have a clean validation result at data entry. 
- Catalogue may provide an interface to the validation results, but it requires a authentication
- Storage

### Key Architectural Decisions

- for the first iteration, Hale Studio was selected, with restricted access to the validation results
- Shacl validation was discussed to be implemented for next iterations (for GeoDCAT-ap and Dublin core)
- minimal SoilWise profile was discussed to indicate compliance with SoilWise functionality
- EUSO Metadata profile was discussed 
- two-step validation of metadata was discussed, at first using harvested metadata, and next using SoilWise-augmented metadata

## Risks & Limitations

- Hale Studio currently does not support Dublin Core
- Users may expect a single validation result and not a historic list of validated sources
- Records from some sources require processing before they can be tested as iso19139 or Dublin core, there is a risk that metadata errors are introduced in pre-processing of the records, in that case the test validates more the software then the metadata itself
