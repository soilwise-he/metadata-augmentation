# streamlit_app.py
import streamlit as st
import spacy_streamlit
import pickle
import spacy
import pandas as pd

from streamlit_tags import st_tags_sidebar,st_tags

import re
import json
import os
from datetime import datetime
import random
import time

st.set_page_config(layout="wide",page_title="Manual NER selection", page_icon="🙌")

ini_label_wanted = ["GPE","coordinate","analysis_method","crop","project_year"]
ini_label_geo=["geo","location","countries","sample_location"]
ini_label_unwanted=["parameter","project_name","organization","soil_type","field","map_location","chemical","scale"]
if 'wanted_labels' not in st.session_state:
    st.session_state.wanted_labels = ini_label_wanted
if 'unwanted_labels' not in st.session_state:
    st.session_state.unwanted_labels = ini_label_unwanted
if 'GEO' not in st.session_state:
    st.session_state.GEO = ini_label_geo
if not 'manual_labels'in st.session_state:
    st.session_state.manual_labels = {} 

geoColor = "#a2f59f"    
colors_list= ["#F6D3BE","#C7B69C","#26858E","#F58154","#F69B46","#2E696A","#FF4500","#FF6347","#FFD700"]


if 'count' not in st.session_state:
    st.session_state.count = 0

@st.cache_data
def importData():
    # Load data from a pickle file
    # with open('training_data.pkl', 'rb') as file:
    #     return pickle.load(file)
    with open('training_data.pkl', 'rb') as file:
        items = pickle.load(file)

    random.shuffle(items)
    
    # Shuffle the list of dictionaries
    return items

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

# Function to save dictionary with a unique filename
def save_dict_to_json(dictionary, directory='saved_dicts',title='dict'):
    # Ensure the directory exists
    if not os.path.exists(directory):
        os.makedirs(directory)
    
    # Generate a unique filename using the current timestamp and a unique identifier
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    filename = f"{timestamp}_{title}.json"
    
    # Full path to save the file
    filepath = os.path.join(directory, filename)
    
    # Save the dictionary to a JSON file
    with open(filepath, 'w') as json_file:
        json.dump(dictionary, json_file, indent=4)
    

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
#data= data_raw[:200]
data=data_raw.copy()



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

# Save annotations
if st.button("💾 Save"):
    save_dict_to_json(st.session_state.manual_labels,directory='data',title='manual-labels')
    st.session_state.manual_labels = {}

# current text
tekst = data[st.session_state.count]['text']
id = data[st.session_state.count]['id']

# NLP process
doc, labels_gliner = make_interpretation([data[st.session_state.count]],st.session_state.wanted_labels+st.session_state.unwanted_labels+st.session_state.GEO)



color_dict={}
color_count = len(colors_list)
for i, key in enumerate(st.session_state.wanted_labels):
    color_dict[key] = colors_list[i % color_count]
for i, key in enumerate(st.session_state.GEO):
    color_dict[key] = geoColor


col4, col5= st.columns(2)

with col4:
    spacy_streamlit.visualize_ner(doc[0],
                                labels=labels_gliner,
                                displacy_options={
                                                "colors": color_dict,
                                                    },
                                )

kolom = ["text","label_"]
labeled_data = [[str(getattr(ent, attr)) for attr in kolom]for ent in doc[0].ents]
df = pd.DataFrame(labeled_data, columns=kolom)
filtered_df = df[df['label_'].isin(st.session_state.GEO)] 

with col5:
    st.header('')
    st.text('')
    st.text('')
    st.text('')
    st.text('')
    
    with st.form("my_form"):



        filtered_df['Check'] = False
        selected_df = st.data_editor(
                filtered_df,
                column_config={
                    "Check": st.column_config.CheckboxColumn(
                        "",
                        help="Duid de locaties aan die **correct** zijn",
                        default=False,
                    )
                },
                disabled=['label_','text'],
                hide_index=True,
                use_container_width=True,
                height =(len(filtered_df)+1)*35
            )

        # Opsplitsen van tekstdocument
        pattern = r"[ ,.!;:]+"
        result = re.split(pattern, tekst)
        result = [word for word in result if word]
        unique_words_in_doc = list(dict.fromkeys(result))


        extra_tags = st_tags(
                        label = '## Vul aan met overige locaties. \n vergeet zeker niet op **enter** te drukken na elke locatie',
                        suggestions = unique_words_in_doc
                        )
        
        submitted = st.form_submit_button("Submit")
        if submitted:
            selected_locations = selected_df[selected_df['Check']]['text'].tolist()+extra_tags
            st.write("locations: ", selected_locations, "id: ", id, "tekst: ", tekst)
            st.session_state.manual_labels[id]={'labels':selected_locations,'text':tekst}

            time.sleep(0.5)

            next_text()
            st.rerun()