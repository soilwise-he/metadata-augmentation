# match the records with the concepts

# result: database table: harvest.match, file: match.json
# other output: mis_keys.json, vague_match.csv

import sys
sys.path.append('utils')

from database import dbQuery, insertMatch, deleteTable

from dotenv import load_dotenv

from rdflib import Graph

from thefuzz import fuzz

import json, csv

from collections import Counter

load_dotenv()

vague_match = []

def turple2dict(rows): # transform a query result from turple to dict
    col_names = ['identifier', 'hash', 'uri', 'turtle']  
    return [dict(zip(col_names, row)) for row in rows]

def formatString(input_string): # remove prefix numbers from a label
    stripped_string = input_string.lstrip()
    
    # Check if the stripped string starts with a number
    if stripped_string and stripped_string[0].isdigit():
        # Remove leading numbers
        return stripped_string.lstrip('0123456789')
    
    return input_string.lstrip()

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


def label_fuzzmatch(subs, keyword, threshold=80):
    

    key_value, key_lang = keyword
    key_value = formatString(key_value) # remove the number prefix
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


def miskeys2csv(mkeys, output_file):
    en_keys = [item[0].lower() for item in mkeys if item[1] == "en"] # lower case
    label_counts = Counter(en_keys)

    sorted_labels = sorted(label_counts.items(), key=lambda x: x[1], reverse=True)

    with open (output_file, 'w', newline='') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(["Label", "Count"， "Remarks"])
        for label, count in sorted_labels:
            csvwriter.writerow([label, count, None])

def main():
    # clear the match table
    sql = '''
    TRUNCATE TABLE harvest.match;
    '''
    result = dbQuery(sql, hasoutput=False)

    # find the records that contain keywords
    sql = '''
    SELECT * FROM harvest.item_contain_keyword;
    '''
    result = turple2dict(dbQuery(sql, hasoutput=True))

    # get defined Concepts
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
    # write output to json file
    with open("./keyword-matcher/match.json", "w", encoding='utf-8') as json_file:
        json.dump(matched_data, json_file, ensure_ascii=False, indent=2) 

    # analyze the mismatched keywords
    miskeys2csv(mismatched_keys, "./keyword-matcher/unmatched_terms.csv")

    # vague match terms
    with open('./keyword-matcher/vague_match.csv', 'w', newline='') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerows(vague_match)
    
    # add code here to update terms.csv

    print("match records successfully, find " + str(len(matched_data)) + " matches")

if __name__ == "__main__":
    main()
    