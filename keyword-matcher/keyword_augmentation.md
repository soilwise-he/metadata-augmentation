# Keyword-Matcher and Keyword-Extractor

## Objective
To harmonize metatdata keywords by a vocabulary for a better findability.

### Catalogue
Use of keyword-augmentation in catalogue: facet search, augmented keywords field (pictures).

### Soilvoc
why use soilvoc: controlled list, cover concepts of soil phenomena, linked with other vocabs for synonyms, definitions support semantic match.

## Architecture

### Database structure

### Workflow:
keyword-matcher: subjects (label, url) - vocab (label, borrowed label, mt-label, definition)

keyword-extractor: records (title, abstract) - vocab (label)

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


