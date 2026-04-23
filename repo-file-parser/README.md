# Repo-file-parser

A common challenge in the fair repository ecosystem is that the identifier of a resource links to a landing page of the resource, 
not to the resource itself, each repository implements a tailored mechanism to navigate from the landing page to the resource(s). 
Zenodo for example advertises the resource links of a record via a dedicated json api at https://zenodo.org/api/records/XXXXX/files.

There’s no universal standard (yet), so the practical approach is a resolver framework with per-repository adapters.
Some academic repositories have implemented a convention to reference a pdf download of an academic article through a 
`citation_pdf_url` header in the landingpage head section.

## Run script (directly or from container)

- set database connection details via environment variables or .env file. 
- python repo-file-parser/resolve.py

## Database storage

Extracted file links are stored in a table. Below is the sql to create the table.

```sql
CREATE TABLE metadata.fileresolver_results (
    id SERIAL PRIMARY KEY,
    doi TEXT, 
    file_url TEXT, 
    insert_date TIMESTAMP
)
```

