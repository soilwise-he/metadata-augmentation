# match the records with the concepts

# when updates on the knowlewdge graph: delete the match table and run match.py
# When update on records: run match.py

import sys
sys.path.append('utils')

from database import dbQuery, insertMatch

from dotenv import load_dotenv

from rdflib import Graph

from thefuzz import fuzz
import json, csv

load_dotenv()

def turple2dict(rows): # transform a query result from turple to dict
    col_names = ['identifier', 'hash', 'uri', 'turtle']  
    return [dict(zip(col_names, row)) for row in rows]


def rdfSearchKeys(rdf):
    # When there is syntex error in the rdf?
    # When keyword not found?
    try:
        g = Graph()
        g.parse(data=rdf, format="turtle")
        query = '''
        PREFIX DCAT: <http://www.w3.org/ns/dcat#>
        SELECT ?keyword ?lang
        WHERE {
            ?s DCAT:keyword ?keyword .
            BIND(lang(?keyword) AS ?lang)
        }
        '''
        results = g.query(query)
        
        # Convert results to a list of lists (keyword, language)
        keywords = [[str(row[0]), str(row[1])] for row in results]
        return keywords
    
    except:
        print(f"Error in RDF parsing or query")
        return []
    
def rdfSearchDctSub(rdf):
    try:
        rdf_pre = 'PREFIX  dct:  <http://purl.org/dc/terms/> ' + rdf
        g = Graph()
        g.parse(data=rdf_pre, format="turtle")

        query = '''
        SELECT ?keyword
        WHERE {
            ?p dct:subject ?keyword
        }
        ''' 
        results = g.query(query)
        keywords = [[str(row[0]), 'en'] for row in results]
        return keywords
    
    except:
        # print(f"Error in RDF parsing or query")
        return []
    
def rdfSearchSubThes(rdf):
    try:
        g = Graph()
        g.parse(data=rdf, format="turtle")
        
        query = '''
        SELECT ?theme
        WHERE {
            { ?s dcat:theme ?theme . } UNION { ?s dcterms:subject ?theme .}
        FILTER(isUri(?theme))
        }
        ''' 
        results = g.query(query)
        themes = [str(row[0]) for row in results]
        themes = list(set(themes)) #get unique values (sometimes duplications in themes and subjects)
        return themes
    
    except:
        # print(f"Error in RDF parsing or query")
        return []

# To find the most matching subject (if exsists) by the labels

vague_match = []

def label_fuzzmatch(subs, keyword, threshold=80):
    
    key_value, key_lang = keyword
    best_match_subject = None
    
    flag = 0

    for sub in subs:
        subject_id = sub['identifier']
        label_dict = sub['labels']
        
        # Flatten labels: include all labels in 'en' and the specified language (if exists)
        # all_labels = labels.get('en', [])  # Default to 'en' labels
        # if key_lang in labels and key_lang != 'en':
        #     all_labels.extend(labels[key_lang])  # Add labels for the specified language
        all_labels = [label for labels in label_dict.values() for label in labels]
        
        sub_flag = 0
        # get the highest matched value of this sub
        for lab in all_labels:
            matched_ratio = fuzz.ratio(key_value.lower(), lab.lower()) # if case sensitive. fuzz.ratio ("water", "water") = 80, short words can have a low match score
            if matched_ratio > sub_flag:
                sub_flag = matched_ratio
        
        # get the highest matched subject and the matched value
        if sub_flag > flag:
            flag = sub_flag
            best_match_subject = subject_id

        
    # check if the matching value with the threshold
    if flag >= threshold:
        #print('find matched label:', key_value, best_match_subject, flag)
        if flag < 100:
            list = [keyword, best_match_subject, flag]
            if list not in vague_match:
                vague_match.append(list)
        return best_match_subject
    else:
        return None


def url_match(subs, themes):
    matched_subs = []

    for sub in subs:
        sub_id = sub['identifier']
        sub_urls = sub['relevant_uris']
        
        if any(url in themes for url in sub_urls):
            matched_subs.append(sub_id)
    
    return matched_subs


sql = '''
SELECT * FROM harvest.item_contain_keyword
'''
result = turple2dict(dbQuery(sql, hasoutput=True))
# for quick testing
# result = result[:100]

# add code here to extract records which not exsists in the matabl table, and process those records

# get defined subjects
with open("./keyword-matcher/concepts.json", "r") as f:
    subs = json.load(f)

matched_data = []
mismatched_keys = []


for res in result:
    turtle = res['turtle']
    themes = []

    if 'dct:subject' in turtle:
        terms = rdfSearchDctSub(turtle)
        keys = [term for term in terms if not term[0].startswith('http')]
        themes = [term[0] for term in terms if term[0].startswith('http')]
    
    else:
        keys = rdfSearchKeys(turtle)
        themes = rdfSearchSubThes(turtle)

    subs_related = []

    # match the keys
    for key in keys:
        sub_key = label_fuzzmatch(subs, key, threshold = 80)
        if sub_key is not None:
            subs_related.append(sub_key)
        else:
            mismatched_keys.append(key)

    # match the themes
    sub_theme = url_match(subs, themes)
    subs_related.extend(sub_theme)

    # get all the matched subject id
    unique_subs_related = list(set(subs_related))

    if len(unique_subs_related) > 0:
        for sub_id in unique_subs_related:
            # Find the corresponding subject by subject_id
            matched_subject = next((sub for sub in subs if sub['identifier'] == sub_id), None)
            if matched_subject:
                insertMatch(res['identifier'], res['hash'], sub_id, matched_subject["labels"]["en"][0] )
                matched_data.append({
                    'record_identifier': res['identifier'],
                    'hash': res['hash'],
                    'concept_identifier': sub_id,
                    'label': matched_subject["labels"]["en"][0]
                })

#print(matched_data)
with open("./keyword-matcher/match.json", "w", encoding='utf-8') as json_file:
    json.dump(matched_data, json_file, ensure_ascii=False, indent=2) 

# analyze the mismatched keywords
with open("./keyword-matcher/mis_keys.json", "w", encoding='utf-8') as json_file:
    json.dump(mismatched_keys, json_file, ensure_ascii=False, indent=2) 

with open('./keyword-matcher/vague_match.csv', 'w', newline='') as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerows(vague_match)

# def getThesaurus(): # get concepts (uri and label) from knowledge graph. language issue!! has only 10000 records?
#     # query_str = "https://sparql.soilwise-he.containers.wur.nl/sparql/?default-graph-uri=&query=PREFIX+dcat%3A+%3Chttp%3A%2F%2Fwww.w3.org%2Fns%2Fdcat%23%3E+%0D%0Aprefix+skos%3A+%3Chttp%3A%2F%2Fwww.w3.org%2F2004%2F02%2Fskos%2Fcore%23%3E%0D%0A%0D%0Aselect+%3Fconcept+%3Flabel%0D%0Awhere+%7B%0D%0A%3Fconcept+a+skos%3AConcept+%3B%0D%0Askos%3AprefLabel+%3Flabel%0D%0A%7D%0D%0A&format=application%2Fsparql-results%2Bjson&timeout=0&signal_void=on"
#     query_str = "https://sparql.soilwise-he.containers.wur.nl/sparql/?default-graph-uri=&query=prefix+skos%3A+%3Chttp%3A%2F%2Fwww.w3.org%2F2004%2F02%2Fskos%2Fcore%23%3E%0D%0A%0D%0Aselect+%3Fconcept+%3Flabel%0D%0Awhere+%7B%0D%0A%3Fconcept+a+skos%3AConcept+%3B%0D%0Askos%3AprefLabel+%3Flabel%0D%0Afilter+isURI%28%3Fconcept%29%0D%0A%7D&format=application%2Fsparql-results%2Bjson&timeout=0&signal_void=on"
#     resp = requests.get(query_str)
#     if resp.ok:
#         res = resp.json()
#         results = res['results']['bindings']
#         uri_labels = []
#         for r in results:
#             uri = r['concept']['value']
#             label = r['label']['value']
#             uri_labels.append({'uri': uri, 'label':label})
#         return (uri_labels)
#     else:
#         print ('fail to fetch concept')
#         return []


# thes = getThesaurus()

# def searchKeyword(rdf): # find keyword section from a rdf string using regular expression
#     pattern = r'dcat:keyword\s+((?:"[^"]+"@\w{2}\s*(?:,\s*"[^"]+"@\w{2}\s*)*));'
#     keyword_match = re.search(pattern, rdf, re.MULTILINE | re.DOTALL)
#     if keyword_match:
#         keyword_section = keyword_match.group(1)
#         matches = re.findall(r'"([^"]+)"@(\w{2})', keyword_section)
#         keywords = [{"keyword": value, "language": lang} for value, lang in matches]
#         return keywords
#     else:
#         # print("No dcat:keyword section found.")
#         return False


# record_keyword = []

# for res in result:
#     keys = searchKeyword(res['turtle'])
#     if not keys:
#         # print(res['identifier'])
#         continue
    
#     for key in keys:
#         keyword_uri = None

#         for conc in thes:
#             if key['keyword'] == conc['label']:
#                 keyword_uri = conc['uri']

#                 row = {
#                     'identifier': res['identifier'],
#                     'date': res['date'],
#                     'uri': res['uri'],
#                     'keyword_label': key['keyword'],
#                     'lang': key['language'],
#                     'keyword_uri': keyword_uri
#                 }
#                 record_keyword.append(row)
#                 break
#         # if not keyword_uri:
#         #     print(f"No matching concept found for keyword: {key['keyword']}")
#         #     continue
# print(len(record_keyword))
# print(record_keyword[:3])