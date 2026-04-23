# Repo-file-parser

A common challenge in the fair repository ecosystem is that the identifier of a resource links to a landing page of the resource, 
not to the resource itself, each repository implements a tailored mechanism to navigate from the landing page to the resource(s). Zenodo for example advertises the resource links of a record via a dedicated json api at https://zenodo.org/api/records/XXXXX/files.

We’re basically trying to normalize a messy reality: a DOI resolves to a landing page, and every repository (Zenodo, Dataverse, DSpace, Dryad, etc.) exposes file URLs differently. There’s no universal standard yet, so the practical approach is a resolver framework with per-repository adapters.




```sql
CREATE TABLE metadata.fileresolver_results (
    id SERIAL PRIMARY KEY,
    doi TEXT, 
    file_url TEXT, 
    insert_date TIMESTAMP
)
```