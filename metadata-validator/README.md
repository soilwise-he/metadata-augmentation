# Record validator

## Goal

Calculate an indication of completeness of an ingested record

## Mechanism

Record is ingested by pycsw, on the ingested (harmonised) result, run the validation.
Properties are evaluated by their existence resulting in a score.
some records may fail ingest and will not be validated.

| indicator | score | 
| --- | --- | 
| identifier | 10 |
| type | 5 |
| title | 25 |
| language | 5 |
| abstract | 20 |
| keywords | 10 |
| temporal_extent | 5 |
| spatial_extent | 5 |
| license (usage constraints) | 5 |
| organization | 10 |
| lineage | 5 |

## Manual

- set .env to connect to database

```
export POSTGRES_HOST=example.org
export POSTGRES_PORT=5432
export POSTGRES_DB=example
export POSTGRES_USER=example
export POSTGRES_PASSWORD=*****

```

- create table `validated`

| identifier | score | date |
| --- | --- | --- |
| aaa-bbb-ccc | 60 | 2025-01-10 |

- install requirementes.txt

```
pip install -r requirements.txt
```
- run validator

```
python validate.py
```

