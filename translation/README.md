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
        key
        lang_key
        value
        lang_value
        ticket
        date_inserted
        date_updated
    }

```

## actual translations

