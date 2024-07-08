# PRE VYBRANÚ TĚMU a ROK
# EXPORTUJE DATASETY DO CSV A PRIDÁ ATRIBUT 'regional'
# porovnaním rozlhoy vypočítanej z atribútu bbox a veĽkosti krajiny
# %%
# IMPORTS
import json
import os
import pandas as pd
import geopandas as gpd
import folium
from shapely.geometry import Polygon
# %%
with open('../coverage/countries-area.json', 'r', encoding='utf-8') as f:
    states_areas = json.load(f)

with open('../countrylist.json', 'r') as f:
    countries = json.load(f)

# %%
THEME = 'soil'
YEAR = 2022

DATA_DIR = '../SIEUSOILmining'
folders = os.listdir(DATA_DIR)
# FUNCTIONS


def getAreaBounds(area):
    deviation = area / 7.5
    return {
        'min': area - deviation,
        'max': area + deviation
    }


def printMap(gdf):
    m = folium.Map([50.854457, 4.377184], zoom_start=5,
                   tiles='cartodbpositron')
    folium.GeoJson(gdf).add_to(m)
    folium.LatLngPopup().add_to(m)
    m.show_in_browser()


def getArea(c, showMap=False):
    # Create shapely polygon
    try:
        if len(c) != 4 or not all(isinstance(elem, float) for elem in c):
            print(f'{c}')
        X1, Y1, X2, Y2 = c
        polygon = [(X1, Y1), (X2, Y1), (X2, Y2), (X1, Y2)]
        polygon_geom = Polygon(polygon)
        # Create Geopandas dataframe
        poly_df = gpd.GeoDataFrame(
            index=[0], crs='epsg:4326', geometry=[polygon_geom])
        # Reproject to LAEA Europe get a proj in meters
        poly_df = poly_df.to_crs('EPSG:3035')
        if showMap:
            printMap(poly_df)
        return poly_df.area[0] / 1000000
    except Exception as e:
        print(f'{e} - {c}')
        return 0

# %%


df = pd.read_json(
    f'{DATA_DIR}/{YEAR}/{THEME}_response_MINIFIED.json')

df['regional'] = True
# %%
for index, row in df.iterrows():
    if not row['memberStateCountryCode'].lower() in countries:
        df.at[index, 'regional'] = 'skipped'
        print(f'Set row nr. {index} as - skipped')
        continue
    country = countries[row['memberStateCountryCode'].lower()]
    country_area = states_areas[country]
    AREA_BOUNDS = getAreaBounds(country_area)
    if not AREA_BOUNDS:
        continue
    # List of coords in order: west,south,east,north
    c = [float(x) for x in row['geobox'][0].split()]
    area = getArea(c)   # can add boolean to show map
    # Checking only min boundary because BBOX is always bigger
    is_national = AREA_BOUNDS['min'] <= area
    df.at[index, 'regional'] = False if is_national else True


# %%
filtered = df[df['regional'] != 'skipped']

# %%
filtered.to_csv(f'{THEME}-{YEAR}-regionality.csv')
# %%
# EXPORT LEN NARODNYCH
df = pd.read_csv('soil-2022-regionality.csv')
df[df['regional'] == False].to_csv(f'{THEME}-{YEAR}-national.csv')
# %%
