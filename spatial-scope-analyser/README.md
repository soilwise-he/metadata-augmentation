
## Spatial Scope Analyser

A script that analyses the spatial scope of a resource

The bounding box is matched to country bounding boxes

To understand if the dataset has a global, continental, national or regional scope

- Retrieves all datasets (as iso19139 xml) from database (records table joined with augmentations) which:
    - have a bounding box 
    - no spatial scope
    - in iso19139 format
- For each record it compares the boundingbox to country bounding boxes: 
    - if bigger then continents > global
    - If matches a continent > continental
    - if matches a country > national
    - if smaller > regional
- result is written to as an augmentation in a dedicated table

| metadata-uri | metadata-element | source | value | proces | date |
| --- | --- | --- | --- | --- | ---|
| https://geo.fi/data/ee44-aa22-33 | spatial-scope | 16.7,62.2,18,81.5 |  https://inspire.ec.europa.eu/metadata-codelist/SpatialScope/national | spatial-scope-analyser | 2024-07-04 |
| https://geo.fi/data/abc1-ba27-67 | spatial-scope | 17.4,68.2,17.6,71,2 |  https://inspire.ec.europa.eu/metadata-codelist/SpatialScope/regional | spatial-scope-analyser | 2024-07-04 |

## Questions

- Should we combine the work with <https://github.com/soilwise-he/metadata-augmentation/issues/1>? if bbox indicates a certain country or continent, we can add it as a location keyword
- If no bbox provided, in theory we can retrieve it from a linked data file/ows service/pdf content...
- in stead of retrieving as xml, we can also query the bbox directly from the harmonised records, not just the harvested iso19139 records?
- should we consider the local scope (for example boundingbox smaller then 5km?)
- what do we do with resources which have multiple spatial coverages, or a point as spatial coverage
- should this component evaluate only EU datasets?
- what to do with metadata of knowledge resources?
- should we run this component also a second time after metadata is enhanced by AI or other metadata-augmentation components? The ratio of location attribute filled in will be assumably higher...
