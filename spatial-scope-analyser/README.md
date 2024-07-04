
## Spatial Scope Analyser

A script that analyses the spatial scope of a resource

The bounding box is matched to country bounding boxes

To understand if the dataset has a continental, national or regional scope

- Retrieves al datasets from database (records table joined with augmentations) which:
    - have a bounding box 
    - no spatial scope
- For each record it compares the boundingbox to country bounding boxes: 
    - if bigger then continents > global
    - If matches a continent > continental
    - if matches a country > national
    - if smaller > regional
- result is written to as augmentation in a dedicated table

| metadata-uri | metadata-element | source | value | proces |
| --- | --- | --- | --- |
| https://geo.fi/data/ee44-aa22-33 | 16.7,62.2,18,81,5 | spatial-scope | national | spatial-scope-analyser |
| https://geo.fi/data/abc1-ba27-67 | 17.4,68.2,17.6,71,2 | spatial-scope | regional | spatial-scope-analyser |


## Questions

- Should we combine the work with <https://github.com/soilwise-he/metadata-augmentation/issues/1>? if bbox indicates a certain country or continent, we can add it as a location keyword
- If no bbox provided, in theory we can retrieve it from a linked data file/ows service/pdf content...
