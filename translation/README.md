## Translation module

Many records arrive in a local language, we aim to capture at least main properties for the record in english: title, abstract, keywords, lineage, usage constraints

- has a db backend, every translation is captured in a database
- the EU translation service is used, this service returns a asynchronous response to an API endpoint (callback)
- the callback populates the database, next time the translation is available
- make sure that frontend indicates if a string has been machine translated, with option to flag as inappropriate

API documentation <https://language-tools.ec.europa.eu/>

A token for the service is available, ask Nick, RobK or Paul if you need it.

```mermaid
classDiagram
    Translations 
 
    class Translations{
        source
        target
        lang_source # has value 'LD' if lang is not known
        lang_target
        ticket
        date_inserted
        date_updated
    }

```

## Callback API

Install & run:

Set up environment variables

```
POSTGRES_HOST=example
POSTGRES_PORT=5432
POSTGRES_DB=example
POSTGRES_USER=example
POSTGRES_PASSWORD=xxx
```

Install requirements and run api locally
```
pip install -r requirements.txt
cd translation
python3 -m uvicorn api:app --reload --host 0.0.0.0 --port 8000
```


## Error codes

Error codes start with -, following error codes are of interest

| code   | description |
| ---    | --- | 
| -20001 | Invalid source language | 
| -20003 | Invalid target language(s) | 
| -20021 | Text to translate too long | 
| -20028 | Concurrency quota exceeded | 
| -20029 | Document format not supported | 
| -20049 | Language can not be detected | 
