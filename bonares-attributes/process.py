#
# Goal: Extracts attributes information from bonares platform  
# Author: @pvgenuchten
# Date: 11-03-2026
#

import json,sys,os,requests
sys.path.append('utils')
from database import dbInit
from dotenv import load_dotenv
load_dotenv()

# select from database locations to geocode
dbconn = dbInit()
with dbconn.cursor() as cur:
    # records which have no location, have not been processed before, and have a location name or an augmented name is available
    # select non processed items
    cur.execute("""
        SELECT identifier,title FROM metadata.records 
            WHERE
                source='BONARES' 
            AND identifier not in (
                select distinct record_id from attributes 
            )
            AND identifier not in (
                select record_id from metadata.augment_status where process='bonares-attributes'
            ) limit 5""")
    records = cur.fetchall()
for rec in records:
    rec_id, name = rec
    # for each item, fetch attributes

    response = requests.get(f"https://maps.bonares.de/finder/resources/dataform/data/{rec_id}/en")
    json_data = json.loads(response.text)

    fts = {}
    for k,v in json_data.get('bonaresInfo',{}).items():
        if k.startswith('datamodelAttribute'):
            tbl = v.get('tableName')
            if tbl in [None,'']:
                tbl = 'main'
            if not tbl in fts:
                fts[tbl] = []
            fts[tbl].append(v.pop("tableName"))

        # insert result
        with dbconn.cursor() as cur:
            cur.execute("insert into metadata.augments (record_id, property, value, process, date) values (%s,%s,%s,%s,now())", (rec_id, 'spatial', str(bbox), 'spatial-locator'))
            cur.execute("insert into metadata.augment_status (record_id, status, process, date) values (%s,%s,%s,now())", (rec_id,'success','spatial-locator'))
        dbconn.commit()

dbconn.close()
