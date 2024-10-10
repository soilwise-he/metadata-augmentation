# streamlit_app.py
import streamlit as st
import spacy_streamlit
import pickle
import spacy
import pandas as pd

from geopy.geocoders import Nominatim
import folium
from shapely import wkt
from shapely.geometry import Point, Polygon, MultiPolygon
from tqdm import tqdm
from streamlit_tags import st_tags_sidebar

from streamlit_folium import st_folium

st.set_page_config(layout="wide")

ini_label_wanted = ["GPE","coordinate","analysis_method","crop","project_year"]
ini_label_geo=["geo","location","countries","sample_location"]
ini_label_unwanted=["parameter","project_name","organization","soil_type","field","map_location","chemical","scale"]
if 'wanted_labels' not in st.session_state:
    st.session_state.wanted_labels = ini_label_wanted
if 'unwanted_labels' not in st.session_state:
    st.session_state.unwanted_labels = ini_label_unwanted
if 'GEO' not in st.session_state:
    st.session_state.GEO = ini_label_geo

geoColor = "#a2f59f"    
colors_list= ["#F6D3BE","#C7B69C","#26858E","#F58154","#F69B46","#2E696A","#FF4500","#FF6347","#FFD700"]

if 'count' not in st.session_state:
    st.session_state.count = 0

@st.cache_data 
def importData():
    # Load data from a pickle file
    with open('training_data.pkl', 'rb') as file:
        return pickle.load(file)

def next_text():

    if st.session_state.count + 1 >= len(data):
        st.session_state.count = 0
    else:
        st.session_state.count += 1

def previous_text():
    if st.session_state.count > 0:
        st.session_state.count -= 1

@st.cache_data
def loadmodel(labellist):
    nlp = spacy.load('en_core_web_sm')
    nlp.add_pipe("gliner_spacy",
        config={
            "labels":labellist
        }, last=True)
    return nlp

@st.cache_data
def make_interpretation(dataset_text,labellist):

    nlp = loadmodel(labellist)
    docs=[]
    my_bar = st.progress(0, text="Finding your entities")
    max_data = len(dataset_text)
    for i,data in enumerate(dataset_text):
        my_bar.progress(i/max_data, text="{:.0f} percent voltooid".format(i/max_data*100))
        doc = nlp(data['text'])
        docs.append(doc)
    my_bar.empty()

    labels=nlp.pipe_labels["gliner_spacy"]
    return docs,labels


def convert_NER_to_geom(NLP_docs,ent_list):
    # Initialize the geocoder
    #geolocator = Nominatim(user_agent="SoilWise_minerApp") # Nominatim: This is the debugging interface for the search engine used on the OpenStreetMap website.

    object_list=[]
    for doc in NLP_docs:
        geom={}
        for ent in doc.ents:
            if not ent.label_ in ent_list:
                continue
            location_obj = geolocator.geocode(ent.text)

            if not location_obj:
                continue
            geom[ent.text] = location_obj
        object_list.append(geom)
    return object_list

st.session_state.wanted_labels = st_tags_sidebar(
                                        label='NER label primair',
                                        text='Press enter to add more',
                                        value=ini_label_wanted,
                                        suggestions=ini_label_wanted,
                                        key='1')
st.session_state.unwanted_labels = st_tags_sidebar(
                                        label='NER label secundair',
                                        text='Press enter to add more',
                                        value=ini_label_unwanted,
                                        suggestions=ini_label_unwanted,
                                        key='2')
st.session_state.GEO = st_tags_sidebar(
                                        label='GEO label',
                                        text='Press enter to add more',
                                        value=ini_label_geo,
                                        suggestions=ini_label_geo,
                                        key='3')



# Initialise data
data_raw = importData()
data= data_raw[:200]

# Scroll buttons
col1, col2, col3 = st.columns((3, 10, 3))

with col2:
    st.session_state.count = st.slider('Current item', 1, len(data)-1, st.session_state.count,label_visibility='collapsed')

with col1:
    if st.button("⏮️ Previous", on_click=previous_text):
        pass

with col3:
    if st.button("Next ⏭️", on_click=next_text):
        pass


# current text
text = data[st.session_state.count]['text']

# NLP process
doc, labels_gliner = make_interpretation([data[st.session_state.count]],st.session_state.wanted_labels+st.session_state.unwanted_labels+st.session_state.GEO)
# docs_geom = convert_NER_to_geom(docs,["geo","location","GPE","countries"])


# doc = docs[st.session_state.count]
# geom = docs_geom[st.session_state.count]



color_dict={}
color_count = len(colors_list)
for i, key in enumerate(st.session_state.wanted_labels):
    color_dict[key] = colors_list[i % color_count]
for i, key in enumerate(st.session_state.GEO):
    color_dict[key] = geoColor

spacy_streamlit.visualize_ner(doc[0],
                              labels=labels_gliner,
                              displacy_options={
                                            "colors": color_dict,
                                                },
                              color_table =st.session_state.GEO,
                              )

kolom = ["text","label_"]
labeled_data = [[str(getattr(ent, attr)) for attr in kolom]for ent in doc[0].ents]
df = pd.DataFrame(labeled_data, columns=kolom)
filtered_df = df[df['label_'].isin(st.session_state.GEO)] 

if len(filtered_df)>0:
    if st.button("🌍 MAPit", type="primary"):
        # Initialize the geocoder
        geolocator = Nominatim(user_agent="SWR") # Nominatim: This is the debugging interface for the search engine used on the OpenStreetMap website.
        # geolocator = GeoNames(username="max_vlaanderen") # GeoNames: https://www.geonames.org/export/geonames-search.html

        # Initialize a map
        map_ = folium.Map(location=[20, 0],
                        zoom_start=2,
                        tiles='https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png',
                        attr='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap contributors</a>')


        # Geocode and plot each location
        for location in tqdm(filtered_df['text'].unique()):
            location_obj = geolocator.geocode(location, geometry='wkt')
            if location_obj:
                st.info(location+' => '+location_obj.address)
                # st.info(location_obj.raw)
            else:
                st.info(location+' : -')
            if location_obj:
                # centerpoint marker
                folium.Marker(
                    location=[location_obj.latitude, location_obj.longitude],
                    popup=location_obj.address,
                    icon=folium.Icon(color="green")
                ).add_to(map_)

                # WKT geometry
                wkt_string=location_obj.raw["geotext"]
                # Parse the WKT string
                geometry = wkt.loads(wkt_string)
                #polygon = polygon.simplify(tolerance=0.0001, preserve_topology=True)

                # Check if geometry is a Point
                if isinstance(geometry, Point):
                    continue
                
                if isinstance(geometry, Polygon):
                    # Convert polygon coordinates to folium-friendly format
                    polygon_coords = [(coord[1], coord[0]) for coord in geometry.exterior.coords]
                

                    # Add the polygon to the map
                    folium.Polygon(
                        locations=polygon_coords,
                        color='green',
                        fill=True,
                        fill_opacity=0.2
                    ).add_to(map_)
                
                elif isinstance(geometry, MultiPolygon):
                    for polygon in geometry.geoms:
                        polygon_coords = [(coord[1], coord[0]) for coord in polygon.exterior.coords]
                        folium.Polygon(
                            locations=polygon_coords,
                            color='green',
                            fill=True,
                            fill_opacity=0.2
                        ).add_to(map_)
                
        # show map
        with st.form(key='myform'):
            st_data = st_folium(map_, width=700, height=500)
            submitted = st.form_submit_button("Reset")
