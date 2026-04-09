from geopy.geocoders import Nominatim
import json,sys,os
sys.path.append('utils')
from database import dbInit
from dotenv import load_dotenv
load_dotenv()

geolocator = Nominatim(user_agent="Soilwise Spatial Locator")

def summarize_boxes(box1, box2):
    x_min = min(box1[0], box2[0])
    y_min = min(box1[1], box2[1])
    x_max = max(box1[2], box2[2])
    y_max = max(box1[3], box2[3])
    
    return [x_min, y_min, x_max, y_max]

# select from database locations to geocode
dbconn = dbInit()
with dbconn.cursor() as cur:
    # records which have no location, have not been processed before, and have a location name or an augmented name is available
    cur.execute("""
        SELECT identifier, spatial, (
                select value from metadata.augments
                where record_id = identifier 
                and process='NER-augmentation'
                Limit 1
            ) FROM metadata.records 
            WHERE spatial not like '[%]'  
            AND coalesce(spatial,'') <> '' 
            AND identifier not in (
                select record_id from metadata.augment_status where process='spatial-locator'
            ) limit 5""")
    records = cur.fetchall()
for rec in records:
    try:
        rec_id, loc_name = rec
        print(f"Geocoding location id {rec_id}: {loc_name}")
        bbox_ = None
        for l in loc_name.split(','):
            location = geolocator.geocode(l, exactly_one=True)
            if location:
                south, north, west, east = map(float, location.raw["boundingbox"])
                bbox = [west, south, east, north]  # [west, south, east, north]
                print('b',bbox)
                if len(bbox) == 4 and bbox[3]-bbox[1] > 0:
                    if bbox_:
                        bbox_ = summarize_boxes(bbox_,bbox)
                    else:
                        bbox_ = bbox

        # update database with bbox
        with dbconn.cursor() as cur:
            if len(bbox_) == 4 and bbox_[3]-bbox_[1] > 0:
                cur.execute("insert into metadata.augments (record_id, property, value, process, date) values (%s,%s,%s,%s,now())", (rec_id, 'spatial', str(bbox), 'spatial-locator'))
                cur.execute("insert into metadata.augment_status (record_id, status, process, date) values (%s,%s,%s,now())", (rec_id,'success','spatial-locator'))
            else:
                raise(f"Empty bbox for {loc_name}")
            
              
    except Exception as e:
        with dbconn.cursor() as cur:
            cur.execute("insert into metadata.augment_status (record_id, status, process, details, date) values (%s,%s,%s,%s,now())", (rec_id,'failed','spatial-locator',e))
               
    dbconn.commit()

dbconn.close()