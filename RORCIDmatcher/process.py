import requests,logging,time
from psycopg2.extras import DictCursor
from datetime import date
from dotenv import load_dotenv
import sys
sys.path.append('utils')
from database import dbInit

# Load environment variables from .env file
load_dotenv()

ORCID_BASE = "https://pub.orcid.org/v3.0"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

logger = logging.getLogger(__name__)

HEADERS = {
    "Accept": "application/json",
    "User-Agent": "Soilwise-HE Augment; Contact: info@soilwise-he.eu"
}

# -----------------------------
# Helpers
# -----------------------------
def parse_orcid_date(date_obj):
    """Convert ORCID date object to Python date."""
    if not date_obj:
        return None

    try:
        year = int(date_obj.get("year", {}).get("value", 1))
        month_ = date_obj.get("month")
        if month_ in [None,'',0]:
            month_ = {"value":1}
        month = int(month_.get("value", 1))
        day_ = date_obj.get("day")
        if day_ in [None,'',0]:
            day_ = {"value":1}
        day = int(day_.get("value", 1))
        return date(year, month, day)
    
    except:
        print('Failed parsing', date_obj)
        return None

def get_or_create_organization(conn, org_name, city=None, country=None, region=None, url=None, ror=None):
    with conn.cursor() as cur:
        # Try find existing
        if ror:
            cur.execute(
                "SELECT id FROM metadata.organization WHERE ror=%s",
                (ror,)
            )
            row = cur.fetchone()
            if row:
                return row[0]
        if org_name:
            cur.execute(
            "SELECT id FROM metadata.organization WHERE lower(name)=%s or %s ilike lower(alias)",
            (org_name.lower(),f'%{org_name}%')
            )
            row = cur.fetchone()
            if row:
                return row[0]

        # Insert new organization
        cur.execute(
            """INSERT INTO metadata.organization (
                name, city, administrativearea, country, ror
            ) VALUES (
               %s,%s,%s,%s,%s
            ) RETURNING id""",
            (org_name, city, region, country, ror)
        )
        return cur.fetchone()[0]


def insert_employment(conn, person_id, organization_id, start_date, end_date, role):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO metadata.employment
            (person_id, organization_id, start_date, end_date, role, source)
            VALUES (%s, %s, %s, %s, %s, 'orcid')
            ON CONFLICT DO NOTHING
            """,
            (person_id, organization_id, start_date, end_date, role)
        )


def resolve_ror(org_name):
    url = "https://api.ror.org/organizations"
    params = {"query": org_name}
    r = requests.get(url, params=params)

    if r.status_code == 200:
        data = r.json()
        if data["items"]:
            # todo: a reverse match, check if country/website matches?
            return data["items"][0]["id"]  # best match
    
    return None

def fetch_orcid_employments(orcid, max_retries=3):
    url = f"{ORCID_BASE}/{orcid}/employments"

    for attempt in range(1, max_retries + 1):
        try:
            logger.info(f"Fetching ORCID {orcid} (attempt {attempt})")

            response = requests.get(
                url,
                headers=HEADERS,
                timeout=15
            )

            # Success
            if response.status_code == 200:
                return response.json()
            
            # Log detailed failure info
            logger.error(
                "ORCID fetch failed | ORCID=%s | Status=%s",
                orcid,
                response.status_code
            )

            # not found
            if response.status_code == 404:
                # report not found error
                return 404

            logger.error(
                "Response headers: %s",
                dict(response.headers)
            )

            # Truncate body to avoid huge logs
            body_preview = response.text[:200]
            logger.error(
                "Response body (first 200 chars): %s",
                body_preview
            )

            # Handle rate limiting specifically
            if response.status_code == 429:
                retry_after = response.headers.get("Retry-After", "5")
                wait_time = int(retry_after)
                logger.warning(
                    "Rate limited. Waiting %s seconds before retry.",
                    wait_time
                )
                time.sleep(wait_time)
                continue

            # Retry on 5xx
            if 500 <= response.status_code < 600:
                logger.warning("Server error, retrying...")
                time.sleep(2 ** attempt)
                continue

            # Do not retry on 4xx (except 429 handled above)
            break

        except requests.exceptions.Timeout:
            logger.error("Timeout while fetching ORCID %s", orcid)
        except requests.exceptions.ConnectionError as e:
            logger.error("Connection error for ORCID %s: %s", orcid, str(e))
        except requests.exceptions.RequestException as e:
            logger.error("Unexpected request error for ORCID %s: %s", orcid, str(e))

        time.sleep(2 ** attempt)

    logger.error("Final failure fetching ORCID %s", orcid)
    return None

def extract_employments(data):
    employments = []

    for group in data.get("affiliation-group", []):
        for summary_wrapper in group.get("summaries", []):
            employment = summary_wrapper.get("employment-summary")
            if employment:
                employments.append(employment)

    return employments


# -----------------------------
# Main logic
# -----------------------------
def process_person(conn, person_id, orcid):
    print(f"Processing person {orcid}")

    data = fetch_orcid_employments(orcid.split('/').pop())
    
    if not data:
        return
    
    # record not found case, set to Null, so it won't be processed again
    if data==404:
        with conn.cursor() as cur:
            cur.execute("update metadata.person set orcid = Null where id=%s",(person_id,))
        return        

    employments = extract_employments(data)

    for summary in employments:
        
        org = summary.get("organization", {})
        org_name = org.get("name")

        if not org_name:
            continue

        city = org.get("address", {}).get("city",'')
        region = org.get("address", {}).get("region",'')
        country = org.get("address", {}).get("country",'')

        role = summary.get("role-title",'')

        start_date = parse_orcid_date(summary.get("start-date"))
        end_date = parse_orcid_date(summary.get("end-date"))

        disamb = summary.get("disambiguated-organization", {})
        ror_ = ''
        if disamb.get('disambiguation-source','') == 'ROR':
            ror_ = disamb.get('disambiguated-organization-identifier','')
        else:
            try:
                ror_ = resolve_ror(org_name)
            except Exception as e:
                print('Failed fetch ROR for',org_name,e)

        organization_id = get_or_create_organization(
            conn,
            org_name,
            city,
            region,
            country,
            ror_
        )

        insert_employment(
            conn,
            person_id,
            organization_id,
            start_date,
            end_date,
            role
        )

    conn.commit()


def main():
    conn = dbInit()

    with conn.cursor(cursor_factory=DictCursor) as cur:
        # if people change jobs, this needs to be force triggered, else all persons are requested every time 
        # limit to 1000 per day, due to rate limiting of orchid
        cur.execute("SELECT id, orcid FROM metadata.person p where coalesce(p.orcid,'') != '' and p.id not in (select person_id from metadata.employment) limit 1000")
        persons = cur.fetchall()

    for person in persons:
        process_person(conn, person["id"], person["orcid"].split('/').pop())

    conn.close()


if __name__ == "__main__":
    main()