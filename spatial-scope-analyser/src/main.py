from dotenv import load_dotenv
import xml.etree.ElementTree as ET
from shapely.geometry import Polygon, Point, box
import psycopg2.extras
import geopandas as gpd
from pyproj import CRS
import pandas as pd
import psycopg2
import time
import os

# Load environment variables from .env file
load_dotenv()

# Load world dataset from local shapefile
world = gpd.read_file("./world_countries/ne_110m_admin_0_countries.shp")

# Load counties details
eu_countries_df = pd.read_csv("./eu_countries.csv")

world = world[world['NAME'].isin(eu_countries_df["Country Name"])]

def setup_database():
    conn = psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST"),
        port=os.environ.get("POSTGRES_PORT"),
        dbname=os.environ.get("POSTGRES_DB"),
        user=os.environ.get("POSTGRES_USER"),
        password=os.environ.get("POSTGRES_PASSWORD")
    )
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    return conn, cur

def get_harvest_records(conn, item_type):
    with conn.cursor() as cur:
        # Fetch identifier and resultobject from harvesters table
        cur.execute("SELECT identifier, resultobject FROM harvest.items WHERE itemtype = %s",(item_type,))
        records = cur.fetchall()

        # Convert the result to Pandas Dataframe
        df = pd.DataFrame(records, columns=['identifier', 'resultobject'])

        return df

def parse_xml(xml_string):
    root = ET.fromstring(xml_string)

    ns = {'gmd': 'http://www.isotc211.org/2005/gmd',
          'gco': 'http://www.isotc211.org/2005/gco'}

    bbox = root.find('.//gmd:EX_GeographicBoundingBox', ns)

    coords = None

    if bbox is not None:
        west = float(bbox.find('./gmd:westBoundLongitude/gco:Decimal', ns).text)
        east = float(bbox.find('./gmd:eastBoundLongitude/gco:Decimal', ns).text)
        south = float(bbox.find('./gmd:southBoundLatitude/gco:Decimal', ns).text)
        north = float(bbox.find('./gmd:northBoundLatitude/gco:Decimal', ns).text)
        coords = (west, south, east, north)

    # print("Coordinates",coords)
    return coords

def get_area(coords):
    west, south, east, north = coords
    polygon = [(west, south), (east, south), (east, north), (west, north)]
    polygon_geom = Polygon(polygon)
    # This creates a GeoDataFrame setting the Global Coordinate System
    poly_df = gpd.GeoDataFrame(index=[0], crs='epsg:4326', geometry=[polygon_geom])
    # Project to european coordinate system since datasets explored are tailored to EU
    poly_df = poly_df.to_crs('EPSG:3035')
    # Divide by 1.000.000 to get it in square kilometers
    return round(poly_df.area[0] / 1000000,3)

def identify_country(west, south, east, north, world):
    try:
        bbox = box(west, south, east, north)
        bbox_gdf = gpd.GeoDataFrame({'geometry': [bbox]}, crs=world.crs)

        # Find intersecting countries
        intersecting = world[world.intersects(bbox)]
        
        if not intersecting.empty:
            # Calculate centroid of the bbox
            bbox_centroid = Point((west + east) / 2, (south + north) / 2)

            # Find countries that contain the centroid
            containing = intersecting[intersecting.contains(bbox_centroid)]

            if not containing.empty:
                return containing.iloc[0]['NAME']
            else:
                # If no country contains the centroid, return the one with the largest overlap
                # Project to Equal Area projection for accurate area calculation
                equal_area_crs = CRS.from_epsg(6933)
                intersecting_projected = intersecting.to_crs(equal_area_crs)
                bbox_projected = bbox_gdf.to_crs(equal_area_crs)
                
                intersections = intersecting_projected.intersection(bbox_projected.iloc[0].geometry)
                max_intersection_idx = intersections.area.idxmax()
                return intersecting.loc[max_intersection_idx, 'NAME']
        
        return None
    except Exception as e:
        print(f"Error in identify_country: {e}")
        return "Error occurred"

def main():
    start_time = time.time()

    print("Connecting to HarvestPostgreSQL db")
    conn, cur = setup_database()

    print("Getting Harvest Records")
    harvest_records = get_harvest_records(conn, 'dataset')

    for index, row in harvest_records.iterrows():
        identifier = row['identifier']
        result_object = row['resultobject']

        coords = parse_xml(result_object)
        
        if coords:
            west, south, east, north = coords
            identified_country = identify_country(west, south, east, north, world)

            if identified_country:
                bounding_box_area = get_area(coords)
                country_area = eu_countries_df.loc[eu_countries_df['Country Name'] == identified_country, 'Area'].values[0]

                # Calculate the ratio of bounding box area to country area
                area_ratio = bounding_box_area / country_area

                # Define a threshold 
                national_threshold = 0.7  # e.g., if the bounding box covers at least 70% of the country

                is_national = area_ratio >= national_threshold

                print(f"Identifier: {identifier}")
                print(f"Identified Country: {identified_country}")
                print(f"Bounding Box Area: {bounding_box_area:.2f} km²")
                print(f"Country Area: {country_area:.2f} km²")
                print(f"Area Ratio: {area_ratio:.2f}")
                print(f"Is National Dataset: {is_national}")
            else:
                print(f"Could not identify a country for identifier: {identifier}")
        else:
            print(f"Missing coordinates for {identifier}")

    cur.close()
    conn.close()

    end_time = time.time()
    print(f"Total execution time: {end_time - start_time:.2f} seconds")

if __name__ == "__main__":
    main()

