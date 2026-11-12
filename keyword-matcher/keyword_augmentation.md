# Keyword-Matcher and Keyword-Extractor

## Objective
The objective is to make records in the soilwise metadata catalogue more FAIR (Findable, Accessible, Interoperable and Reusable) by linking their subject metadata to standard vocabularies such as [SoilVoc](https://w3id.org/eusoilvoc). The keyword matcher aligns keywords already present in a record with relevant SoilVoc concepts, while the keyword extractor identifies relevant concepts from the record’s text when subject metadata is missing or incomplete.

## Architecture

### Database structure

### Workflow:

#### Keyword matcher

For each keyword or subject in a record, identify if it matches with a keyword from a preselected list of relevant terms (SoilVoc).
Matching is based on label translation, synonyms, sameAs/closeMatch/exactMatch relationships.


```mermaid
flowchart TD
    S(["Subject keyword<br/>(URI and/or label)"]) --> U{"Has a URI?"}

    U -- yes --> UM{"URI in a concept's<br/>skos:exactMatch / closeMatch URIs?"}
    UM -- yes --> M100["Match<br/>score = 100"]
    UM -- no --> L
    U -- no --> L

    L{"Has a label?<br/>(after stripping numeric prefix,<br/>e.g. '106022 mikrobiologie')"}
    L -- no --> NM(["No match<br/>score = NULL"])
    L -- yes --> E{"Label equals a concept label?<br/>(case-insensitive, any language:<br/>en, fr, de, it, es, pt, nl)"}
    E -- yes --> M100

    E -- no --> R["Retrieve: bi-encoder embeds the label,<br/>top 15 nearest concept labels (all languages)"]
    R --> F["Keep candidates with cosine ≥ 0.40,<br/>best label per concept"]
    F --> C{"Any candidates left?"}
    C -- no --> NM
    C -- yes --> X["Rerank: cross-encoder scores each pair<br/>(subject label, concept passage)<br/>passage = 'label: definition', or label only"]
    X --> B["Pick the candidate with the<br/>highest cross-encoder score (0–1)"]
    B --> T{"Score ≥ 0.60?"}
    T -- yes --> MCE["Match<br/>score = round(score × 100)"]
    T -- no --> NM

    M100 --> O(["Output: concept ID +<br/>primary label (English first)"])
    MCE --> O

    classDef match fill:#d4edda,stroke:#2e7d32,color:#1b1b1b;
    classDef nomatch fill:#f8d7da,stroke:#c62828,color:#1b1b1b;
    class M100,MCE,O match;
    class NM nomatch;
```


#### Keyword extractor 

The textual content of the record (title, abstract, full text) is analysed to extract relevant keywords from a preselected list of relevant terms (SoilVoc).

## Methods exploration: keyword-matcher
V1.0 (current status):\
Use fuzzy match, few records have matched keywords, FP by fuzzy.

[flowchart]

v2.0 (ongoing work):\
To introduce other matching techniques to support or replace fuzzy.

### Techniques
- Embedding (bi-encoder): ...
- Cross-encoder: ...
- Machine translate: ...

### Approaches
table to explain the 4 approached

### Experiment
Experiment plan. \
Explanation of some decision points.

### Evaluation
Plan, result and conclusion. Use appr4.

## Methods exploration: keyword-extractor
v1.0: (ongoing work)
to enrich the keywords field from metadata (title and abstract)

### Techniques
- Embedding and cross-encoder
- Keybert
- LLM (KeyLLM)

### Experiments and evaluation
...


