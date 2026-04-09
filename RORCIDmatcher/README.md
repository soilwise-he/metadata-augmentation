
## Orcitter

Finds the employments of persons identified by orcid id
These employments are used to match persons to organisations, 
if this info is unavailable on records (for example in OpenAire)

Also tries to resolve a ROR, if it is not provided with the orcid

Updates person, resetting the orcid to Null, if orcid does not exist

Runs 500 records per request, to prevent rate limiting by Orcid

## todo

- use ror to retrieve extra organisation details
- for persons without orcid, try to find matching orcid
