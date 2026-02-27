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
    cur.execute("SELECT id, location_name FROM metadata.records WHERE bbox IS NULL")
    locations = cur.fetchall()
for loc in locations:
    loc_id, loc_name = loc
    print(f"Geocoding location id {loc_id}: {loc_name}")
    location = geolocator.geocode(loc_name, exactly_one=True)
    if location:
        south, north, west, east = map(float, location.raw["boundingbox"])
        bbox = [west, south, east, north]  # [west, south, east, north]
            
        # update database with bbox
        with dbconn.cursor() as cur:
            cur.execute("UPDATE metadata.locations SET bbox = %s WHERE id = %s", (json.dumps(bbox), loc_id))
        dbconn.commit()
    else:
        print(f"Could not geocode location: {loc_name}")

dbconn.close()