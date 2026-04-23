
# query linky database and insert on metadata.pdfitems

import sys
sys.path.append('utils')
from database import dbInit
from dotenv import load_dotenv
load_dotenv()

# select from database locations to geocode
dbconn = dbInit()
records = []
with dbconn.cursor() as cur:
    # select urls of records
    # select non processed items
    cur.execute("""SELECT r.record_id, l.urlname as url 
                FROM linky.links l 
                left join linky.records r on l.fk_record = r.id
                where link_type='application/pdf'
                and deprecated is false
                and r.record_id in (select identifier from metadata.records) limit 1000""")

    records = cur.fetchall()

for rec in records:
    rec_id, url = rec
    rec_id = rec_id.split('items/').pop().split('_')[0]
    try:
        with dbconn.cursor() as cur:
            cur.execute("""insert into metadata.pdf_items ( identifier, pdfurl
                        ) values (%s,%s) ON CONFLICT (identifier, pdfurl) DO Nothing""", (rec_id, url))
            print('insert success',rec_id)
    except Exception as e:
        None

dbconn.commit()
dbconn.close()

print(len(records), 'processed')