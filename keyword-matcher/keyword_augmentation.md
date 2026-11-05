# Keyword-Matcher and Keyword-Extractor

## Objective
To extract and harmonize keywords (subjects) in metadata for improved findability.

## Architecture

### Database structure

### Workflow:

#### Keyword matcher

For each keyword or subject in a record, identify if it matches with a keyword from a preselected list of relevant terms (SoilVoc).
Matching is based on label translation, synonyms, sameAs/closeMatch/exactMatch relationships.

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


