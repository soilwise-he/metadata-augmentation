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
                select distinct record_id from metadata.attributes 
            )
            AND identifier not in (
                select record_id from metadata.augment_status where process='bonares-attributes'
            ) limit 100""")
    records = cur.fetchall()
    for rec in records:
        rec_id, name = rec
        print(name)
        # for each item, fetch attributes

        response = requests.get(f"https://maps.bonares.de/finder/resources/dataform/data/{rec_id}/en")
        
        try:
            json_data = json.loads(response.text)
        except Exception as e:
            print('Error parsing response:', rec_id, e)
            cur.execute("""insert into metadata.augment_status (
                            record_id, status, process, details, date 
                        ) values (%s,%s,%s,%s,now())""", (
                    rec_id,'failed','bonares-attributes', f'{rec_id} parse failed. {e}'))
            continue

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
            if len(fts.keys()) > 0:
                with dbconn.cursor() as cur:
                    cur.execute("insert into metadata.augments (record_id, property, value, process, date) values (%s,%s,%s,%s,now())", (rec_id, 'spatial', json.dumps(fts), 'bonares-attributes'))
                    cur.execute("insert into metadata.augment_status (record_id, status, process, date) values (%s,%s,%s,now())", (rec_id,'success','bonares-attributes'))
                dbconn.commit()

dbconn.close()
