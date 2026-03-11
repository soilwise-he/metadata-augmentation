from geopy.geocoders import Nominatim
import json,sys,os
sys.path.append('utils')
from database import dbInit
from dotenv import load_dotenv
load_dotenv()

geolocator = Nominatim(user_agent="Soilwise Spatial Locator")

# select from database locations to geocode
dbconn = dbInit()
with dbconn.cursor() as cur:
    # records which have no location, have not been processed before, and have a location name or an augmented name is available
    cur.execute("""
        SELECT identifier, spatial, (
            select 
            ) FROM metadata.records 
            WHERE spatial not like '[%]'  
            AND coalesce(spatial,'') <> '' 
            AND identifier not in (
                select record_id from augment_status where process='spatial-locator'
            ) limit 5""")
    records = cur.fetchall()
for rec in records:
    try:
        rec_id, loc_name = rec
        print(f"Geocoding location id {rec_id}: {loc_name}")
        location = geolocator.geocode(loc_name, exactly_one=True)
        if location:
            south, north, west, east = map(float, location.raw["boundingbox"])
            bbox = [west, south, east, north]  # [west, south, east, north]

            # update database with bbox
            with dbconn.cursor() as cur:
                if len(bbox) == 4 and bbox[3]-bbox[1] > 0:
                    cur.execute("insert into metadata.augments (record_id, property, value, process, date) values (%s,%s,%s,%s,now())", (rec_id, 'spatial', str(bbox), 'spatial-locator'))
                    cur.execute("insert into metadata.augment_status (record_id, status, process, date) values (%s,%s,%s,now())", (rec_id,'success','spatial-locator'))
                else:
                    raise(f"Empty bbox for {loc_name}")
        else:
            raise(f"No geonames response for {loc_name}")    
    except Exception as e:
        cur.execute("insert into metadata.augment_status (record_id, status, process, details, date) values (%s,%s,%s,%s,now())", (rec_id,'failed','spatial-locator',e))
               
    dbconn.commit()

dbconn.close()