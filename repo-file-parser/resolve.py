import asyncio, aiohttp, asyncpg
import os, re
from urllib.parse import urlparse
from datetime import datetime
from dotenv import load_dotenv
from bs4 import BeautifulSoup
from urllib.parse import urljoin

# Load environment variables from .env file
load_dotenv()

POSTGRES_HOST=os.environ.get("POSTGRES_HOST", '')
POSTGRES_PORT=os.environ.get("POSTGRES_PORT", '5432')
POSTGRES_DB=os.environ.get("POSTGRES_DB", '')
POSTGRES_USER=os.environ.get("POSTGRES_USER", '')
POSTGRES_PASSWORD=os.environ.get("POSTGRES_PASSWORD", '')

SEM_LIMIT = 10
PIPELINE_VERSION = "v1.0.0"
TIMEOUT = aiohttp.ClientTimeout(
    total=10,        # max total time for request
    connect=5,       # connection phase
    sock_read=5      # reading response
)
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept": "text/html,application/xhtml+xml",
    "Accept-Language": "en-US,en;q=0.9"
}

# ----------------------------
# REPOSITORY DETECTION
# ----------------------------

def detect_repository(landing_url):
    domain = urlparse(landing_url).netloc.lower()

    if "zenodo.org" in domain:
        return "zenodo"
    elif "dataverse" in domain or 'recherche.data.gouv.fr' in domain:
        return "dataverse"
    elif "datadryad.org" in domain:
        return "dryad"
    elif "onlinelibrary.wiley.com" in domain:
        return "wiley"
    elif "link.springer.com" in domain:
        return "springer"
    elif "sciencedirect.com" in domain or "elsevier.com" in domain:
        return "sciencedirect"
    elif "pubs.acs.org" in domain:
        return "acs"
    elif "pubs.rsc.org" in domain:
        return "rsc"
    elif "nature.com" in domain:
        return "nature"
    elif "pnas.org" in domain:
        return "pnas"
    elif "pangaea.de" in domain:
        return "pangaea"
    elif "handle.net" in domain or "dspace" in domain:
        return "dspace"
    return "unknown"


# ----------------------------
# DB: FETCH DOIs
# ----------------------------

async def fetch_dois_from_db(pool):
    query = """
        SELECT identifier as doi
        FROM metadata.records
        WHERE identifier ~ '10\\.[0-9]{4,9}/\\S+'
        and identifier not in (
            select record_id 
            from metadata.augment_status 
            where process='file-resolver')  
        LIMIT 100
    """

    async with pool.acquire() as conn:
        rows = await conn.fetch(query)

    return [r["doi"] for r in rows if r["doi"]]


# ----------------------------
# FETCH HELPERS
# ----------------------------

async def fetch_json(session, url):
    async with session.get(url, timeout=10) as resp:
        return await resp.json()


async def fetch_text(session, url):
    async with session.get(url, timeout=10) as resp:
        return await resp.text()


# ----------------------------
# ADAPTERS
# ----------------------------

async def get_zenodo_files(session, landing_url):
    record_id = landing_url.rstrip("/").split("/")[-1]
    api_url = f"https://zenodo.org/api/records/{record_id}"
    data = await fetch_json(session, api_url)
    return [f["links"]["self"] for f in data.get("files", [])]


async def get_dryad_files(session, landing_url):
    doi = landing_url.split("doi.org/")[-1]
    api_url = f"https://datadryad.org/api/v2/datasets/{doi}"
    data = await fetch_json(session, api_url)
    return [f["_links"]["self"]["href"] for f in data["_embedded"]["files"]]

async def get_wiley_files(session, landing_url):
    # Normalize DOI from URL
    if "/doi/" not in landing_url:
        return []

    doi = landing_url.split("/doi/")[-1]

    pdf_url = f"https://agupubs.onlinelibrary.wiley.com/doi/pdf/{doi}"

    return [pdf_url]

async def get_springer_files(session, landing_url):
    if "/article/" not in landing_url:
        return []

    doi = landing_url.split("/article/")[-1]

    parsed = urlparse(landing_url)
    base = f"{parsed.scheme}://{parsed.netloc}"

    pdf_url = f"{base}/content/pdf/{doi}.pdf"

    return [pdf_url]

async def get_dataverse_files(session, landing_url):
    if "persistentId=" not in landing_url:
        return []

    doi = landing_url.split("persistentId=")[-1]
    base = landing_url.split("/dataset")[0]
    api_url = f"{base}/api/datasets/:persistentId/?persistentId={doi}"

    data = await fetch_json(session, api_url)

    return [
        f"{base}/api/access/datafile/{f['dataFile']['id']}"
        for f in data["data"]["latestVersion"]["files"]
    ]


async def get_pangaea_files(session, landing_url):
    """
    PANGAEA dataset pages list downloadable resources as links.
    """

    html = await fetch_text(session, landing_url)
    soup = BeautifulSoup(html, "html.parser")

    files = []

    # Look for download links
    for a in soup.find_all("a", href=True):
        href = a["href"]

        # Common PANGAEA patterns:
        if any(x in href for x in ["download", "export", "file", "dataset", "txt", "csv", "zip"]):
            files.append(urljoin(landing_url, href))

    # Deduplicate
    return list(set(files))

async def get_sciencedirect_files(session, landing_url):
    # Example:
    # https://www.sciencedirect.com/science/article/pii/S0167880922001234

    match = re.search(r'/pii/([A-Z0-9]+)', landing_url)
    if not match:
        return []

    pii = match.group(1)

    parsed = urlparse(landing_url)
    base = f"{parsed.scheme}://{parsed.netloc}"

    pdf_url = f"{base}/science/article/pii/{pii}/pdfft"

    return [pdf_url]

async def get_acs_files(session, landing_url):
    # Extract DOI from URL
    if "/doi/" not in landing_url:
        return []

    doi = landing_url.split("/doi/")[-1]

    parsed = urlparse(landing_url)
    base = f"{parsed.scheme}://{parsed.netloc}"

    pdf_url = f"{base}/doi/pdf/{doi}"

    return [pdf_url]

async def get_rsc_files(session, landing_url):
    if "/articlelanding/" not in landing_url:
        return []

    pdf_url = landing_url.replace("/articlelanding/", "/articlepdf/")

    return [pdf_url]

async def get_nature_files(session, landing_url):
    if "/articles/" not in landing_url:
        return []

    article_id = landing_url.split("/articles/")[-1]

    parsed = urlparse(landing_url)
    base = f"{parsed.scheme}://{parsed.netloc}"

    pdf_url = f"{base}/articles/{article_id}.pdf"

    return [pdf_url]

async def get_pnas_files(session, landing_url):
    if "/doi/" not in landing_url:
        return []

    doi = landing_url.split("/doi/")[-1]

    parsed = urlparse(landing_url)
    base = f"{parsed.scheme}://{parsed.netloc}"

    pdf_url = f"{base}/doi/pdf/{doi}"

    return [pdf_url]

async def get_dspace_files(session, landing_url):


    html = await fetch_text(session, landing_url)
    soup = BeautifulSoup(html, "html.parser")

    return [
        a.get("href")
        for a in soup.find_all("a")
        if "bitstream" in (a.get("href") or "")
    ]


# ----------------------------
# DOI PROCESSING
# ----------------------------

async def resolve_doi(session, doi):
    url = f"https://doi.org/{doi}"
    async with session.get(url, allow_redirects=True) as resp:
        return str(resp.url)

# can query open access if needed
async def validate_url(session, url):
    async with session.head(url, allow_redirects=True) as resp:
        return resp.status

async def process_doi(session, pool, doi, sem):
    async with sem:
        try:
            landing = await resolve_doi(session, doi)
            repo = detect_repository(landing)
            print(f'parse {doi} as {repo} via {landing}')
            if repo == "zenodo":
                files = await get_zenodo_files(session, landing)
            elif repo == "dryad":
                files = await get_dryad_files(session, landing)
            elif repo == "wiley":
                files = await get_wiley_files(session, landing)
            elif repo == "springer":
                files = await get_springer_files(session, landing)
            elif repo == "sciencedirect":
                files = await get_sciencedirect_files(session, landing)
            elif repo == "acs":
                files = await get_acs_files(session, landing)
            elif repo == "rsc":
                files = await get_rsc_files(session, landing)
            elif repo == "nature":
                files = await get_nature_files(session, landing)
            elif repo == "pnas":
                files = await get_pnas_files(session, landing)
            elif repo == "dataverse":
                files = await get_dataverse_files(session, landing)
            elif repo == "pangaea":
                files = await get_pangaea_files(session, landing)
            elif repo == "dspace":
                files = await get_dspace_files(session, landing)
            else:
                files = await extract_pdf_or_downloads(session, landing)

            await insert_files(pool, doi, files)

        except Exception as e:
            await log_provenance(pool, doi, error=str(e))

async def extract_pdf_or_downloads(session, landing_url):
    """
    Universal extractor:
    1. citation_pdf_url (journals)
    2. schema.org JSON-LD (datasets + hybrid repositories)
    """

    html = await fetch_text(session, landing_url)
    soup = BeautifulSoup(html, "html.parser")

    # -----------------------------------
    # 1. BEST CASE: explicit PDF metadata
    # -----------------------------------
    meta = soup.find("meta", {"name": "citation_pdf_url"})
    if meta and meta.get("content"):
        return [meta["content"]]

    # -----------------------------------
    # 2. SCHEMA.ORG JSON-LD EXTRACTION
    # -----------------------------------
    files = []

    for script in soup.find_all("script", {"type": "application/ld+json"}):
        try:
            data = json.loads(script.string)

            # normalize to list
            nodes = data if isinstance(data, list) else [data]

            for obj in nodes:
                dist = obj.get("distribution", [])

                for d in dist:
                    if isinstance(d, dict):

                        # case A: DataDownload
                        if "contentUrl" in d:
                            files.append(d["contentUrl"])

                        # case B: nested download objects
                        if "downloadUrl" in d:
                            files.append(d["downloadUrl"])

        except Exception:
            continue

    if files:
        return list(set(files))

    # -----------------------------------
    # 3. NOTHING FOUND
    # -----------------------------------
    return []

# ----------------------------
# DATABASE WRITES
# ----------------------------

async def insert_files(pool, doi, files):

    async with pool.acquire() as conn:
        await conn.executemany("""
            INSERT INTO metadata.fileresolver_results (doi, file_url, insert_date)
            VALUES ($1, $2, NOW())
        """, [
            (doi, f)
            for f in files
        ])

        await log_provenance(
            conn,
            doi,
            file_count=len(files),
            status="success"
        )


async def log_provenance(conn_or_pool, doi, **kwargs):
    query = """
        INSERT INTO metadata.augment_status (
            record_id, status, process, details, date
        )
        VALUES ($1, $2, 'file-resolver', $3, NOW())
    """

    values = (
        doi,
        kwargs.get("status", "error"),
        kwargs.get("error")
    )

    if hasattr(conn_or_pool, "execute"):
        await conn_or_pool.execute(query, *values)
    else:
        async with conn_or_pool.acquire() as conn:
            await conn.execute(query, *values)


# ----------------------------
# MAIN
# ----------------------------

async def main():
    pool = await asyncpg.create_pool(
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        database=POSTGRES_DB,
        port=POSTGRES_PORT,
        host=POSTGRES_HOST
    )

    # Fetch DOIs from db (which have not been processed)
    dois = await fetch_dois_from_db(pool)

    print(f"Fetched {len(dois)} DOIs")

    sem = asyncio.Semaphore(SEM_LIMIT)

    async with aiohttp.ClientSession(headers=HEADERS, timeout=TIMEOUT) as session:
        tasks = [
            process_doi(session, pool, doi, sem)
            for doi in dois
        ]

        await asyncio.gather(*tasks)


# Entry point
if __name__ == "__main__":
    asyncio.run(main())