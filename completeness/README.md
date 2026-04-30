# Record validator

## Goal

Calculate an indication of completeness of an ingested record

## Mechanism

Record is ingested by harvesters, on the ingested (harmonised and augmented) result, runs a completeness validation every 10 days.
Properties are evaluated by their existence resulting in a score (up to 100%).
some records may fail ingest and will not be validated.
Below table shows per indicator a weight on the percentage.

| indicator | score | 
| --- | --- | 
| identifier | 10 |
| title | 20 |
| abstract | 10 |
| language | 5 |
| type | 5 |
| thumbnail | 10 |
| date | 5 |
| datamodel | 10 |
| subjects | 10 |
| matched_subjects | 10 |
| contacts | 10 |
| accessconstraints | 5 |
| temporal_start | 5 |
| temporal_end | 5 |
| spatial | 10 |
| distributions | 10 |
| projects | 5 |
| license | 10 |
| rights | 5 |
| format | 5 |
| lineage | 5 |

## Assessing results

Results are stored in table `metadata.augments`. 
Get results:

```sql
select * from metadata.augments
where process='completeness'
```

## Installation and running a task

- set .env to connect to database

```
export POSTGRES_HOST=example.org
export POSTGRES_PORT=5432
export POSTGRES_DB=example
export POSTGRES_USER=example
export POSTGRES_PASSWORD=*****

```

- install requirementes.txt

```
pip install -r requirements.txt
```
- run validator

```
python completeness/validate.py
```

