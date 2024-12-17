
# Design Document: EUOS-high-value dataset tagging

## Introduction

### Component Overview and Scope

The EUSO high-value datasets are those with substantial potential to assess soil health status, as detailed on the [EUSO dashboard](https://esdac.jrc.ec.europa.eu/esdacviewer/euso-dashboard/). This framework includes the concept of soil degradation indicator metadata-based identification and tagging. Each dataset (possibly only those with the supra-national spatial scope - under discussion) will be annotated with a potential soil degradation indicator for which it might be utilised. Users can then filter these datasets according to their specific needs.

The EUSO soil degradation indicators employ specific methodologies and thresholds to determine soil health status

### Users

1. **authorised data users**
2. **unauthorised data users**
3. **administrators**

### References

- [EUSO HIgh-value data methodology](https://esdac.jrc.ec.europa.eu/euso/euso-dashboard-sources)

## Requirements

### Functional Requirements

- For critical / high-value resources, SoilWise will either link with them or store them

### Non-functional Requirements

- Labelling data results as high-value data sets will rely on EUSO/ESDAC criteria, which are not identical to the High Value Data directive criteria.

## Architecture

### Technological Stack

### Overview of Key Features

### Component Diagrams

### Sequence Diagram

``` mermaid
flowchart TD
    subgraph ic[Indicators Search]
        ti([Title Check]) ~~~ ai([Abstract Check])
        ai ~~~ ki([Keywords Check])
    end
    subgraph Codelists
        sd ~~~ si
    end
    subgraph M[Methodologies Search]
        tiM([Title Check]) ~~~ aiM([Abstract Check])
        kl([Links check]) ~~~ kM([Keywords Check])
    end
    m[(Metadata Record)] --> ic
    m --> M
    ic-- + ---M
    sd[(Soil Degradation Codelist)] --> ic
    si[(Soil Indicator Codelist)] --> ic
    em[(EUSO Soil Methodologies list)] --> M
    M --> et{{EUSO High-Value Dataset Tag}}
    et --> m
    ic --> es{{Soil Degradation Indicator Tag}}
    es --> m
    th[(Thesauri)]-- synonyms ---Codelists
```

### Database Design

### Integrations & Interfaces

- [Spatial scope analyser](/metadata-augmentation/tree/main/spatial-scope-analyser/README.md)
- Catalogue
- Knowledge graph

### Key Architectural Decisions

## Risks & Limitations