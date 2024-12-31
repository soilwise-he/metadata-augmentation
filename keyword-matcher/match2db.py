import sys
sys.path.append('utils')

from database import dbQuery, insertMatch, deleteTable

from dotenv import load_dotenv

from rdflib import Graph

from thefuzz import fuzz
import json, csv

def read_csv_to_dict(filename):
    data = []
    with open(filename, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            data.append({key: (value if value.strip() != '' else None) for key, value in row.items()})
    return data

def get_label(data, identifier):
    for item in data:
        if item["identifier"] == identifier:
            return item["label"]
    return None  # Return None if not found

def get_class(t_id, mapping): # get the class by a term id
    result_c = None
    for key, values in mapping.items():
        if t_id in values:
            result_c = key
            break
    return result_c


# load_dotenv()

# # drop materialized view
# sql = '''
# DROP MATERIALIZED VIEW IF EXISTS public.records_terms;
# '''
# result = dbQuery(sql, hasoutput=False)


# create new view
terms = read_csv_to_dict('keyword-matcher/terms.csv')
classes = [item['class'] for item in terms if item['class'] is not None]
# classes_list = [c.replace(" ", "_") for c in classes]
classes_uniq = list(set(classes))
# create a mapping object
mapping = {key: [] for key in classes_uniq}
for t in terms:
    if t['class'] is not None:
        c = t['class']
        i = t['identifier']
        mapping[c].append(i)

c_mapping = {key.replace(" ", "_"): value for key, value in mapping.items()} 


with open("keyword-matcher/match.json", "r") as f:
    match = json.load(f)
match_withterms =[]
keys = ['identifier'] + list(c_mapping.keys())
default_value = None


result = []

# Group matches by record_identifier
records = {}
for m in match:
    record_id = m["record_identifier"]
    term_id = m["concept_identifier"]
    records.setdefault(record_id, []).append(term_id)

for record_id, term_ids in records.items(): # for each record (unique)
    dic = {key: None for key in keys} # initialize the div
    dic['identifier'] = record_id
    for t_id in term_ids: # for each term id of that record
        t_class = get_class(t_id, c_mapping)
        
        if t_class is not None: # find a class
            t_label = get_label(terms, t_id) # get the label from that id
            if t_label is not None:
                if dic[t_class] is None: # if it is the first term of that class
                    dic[t_class] = t_label
                else:
                    dic[t_class] = dic[t_class] + ',' + t_label
    result.append(dic)

print(len(result))

