# import a range of records from a csw endpoint constraint by a filter

from dotenv import load_dotenv
import sys
from datetime import datetime
sys.path.append('utils')
from database import dbQuery

# Load environment variables from .env file
load_dotenv()

# which pycsw fields
# which fields are recommended # todo: could set a score per field


	
recflds={"identifier":10,
         "title":20,
         "abstract":10,
         "language":5,
         "type":5,
         "thumbnail":10,
         "date":5,
         "datamodel":10,
         "subjects":10,
         "matched_subjects":10,
         "contacts":10,
         "accessconstraints":5,
         "temporal_start":5,
         "temporal_end":5,
         "spatial":10,
         "distributions":10,
         "projects":5,
         "license":10,
         "rights":5,
         "format":5,
         "lineage":5 } 

ttl = 0
for c in recflds.values():
    ttl = ttl + c
sfactor = 100/ttl

# todo: some fields may need special processing, such as parse json and check inner value
# todo: some special rules based on type (document/dataset)

# get records which have not been validated for 10 days
recs = dbQuery(f"""SELECT {','.join(recflds.keys())} 
                   FROM metadata.mv_records 
                   WHERE identifier not in (
                     SELECT record_id from metadata.augment_status
                     WHERE process='completeness'
                     AND date > NOW() - INTERVAL '10 DAYS')
                   limit 500""",(),True) 
loaded_files = []

print(f"Found {len(recs)} records to check for completeness")

if recs:
    # if type == dataset -> data specific things such as datamodel
    
    total = len(recs)
    for rec in sorted(recs):
        md={}
        i=0
        for f0 in recflds.keys():
            md[f0] = rec[i]
            i+=1
        score=0
        # run check, which fields are required / optional
        for k,v in recflds.items():
            if md.get(k,'') not in [None,'']:
                score = score + v*sfactor
        # insert score to db
        print(f"Result for {md.get('identifier','')} score {round(score,1)}")

        dbQuery("""INSERT INTO metadata.augment_status(
            record_id, status, process, date, details)
            VALUES (%s, %s, %s, NOW(), %s)""",(md.get('identifier',''),True,'completeness',''),False)
        dbQuery("""INSERT INTO metadata.augments(
            record_id, property, value, process, date)
            VALUES (%s, %s, %s, %s, NOW())""",(md.get('identifier',''), 'completeness', round(score,1), 'completeness'),False) 