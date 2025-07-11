# match the records and dump result to keyword_temp

# input: concept.json, database..., terms.csv
# output: database: public.keyword_temp

import time

import datetime

import sys

sys.path.append('utils')

from database import dbQuery, insertSQL

from dotenv import load_dotenv

from rdflib import Graph

from thefuzz import fuzz

import json, csv

import getopt

import logging

from collections import Counter



def turple2dict(rows): # transform a query result from turple to dict
    col_names = ['identifier', 'hash', 'uri', 'turtle', 'prefix']  
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
    
    except Exception as e:
        logging.error(e)
        return []

    
def rdfSearchSubThes(rdf):
    try:
        g = Graph()
        g.parse(data=rdf, format="turtle")
        
        query = '''
        PREFIX dcat: <http://www.w3.org/ns/dcat#> 
        PREFIX dcterms: <http://purl.org/dc/terms/> 
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
    
    except Exception as e:
        logging.info(e)
        return []

def rdfSearchDctSub(rdf):
    try:
        # rdf_pre = 'PREFIX  dct:  <http://purl.org/dc/terms/> ' + rdf
        g = Graph()
        g.parse(data=rdf, format="turtle")

        query = '''
        prefix dct: <http://purl.org/dc/terms/>
        SELECT ?keyword
        WHERE {
            ?p dct:subject ?keyword
        }
        ''' 
        results = g.query(query)
        keywords = [[str(row[0]), 'en'] for row in results]
        return keywords
    
    except Exception as e:
        logging.info(e)
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
            # if list not in vague_match:
            #     vague_match.append(list)
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
            return item["label"].lower() # transform to lower case
    return None  # Return None if not found

def get_class(t_id, mapping): # get the class by a term id
    result_c = None
    for key, values in mapping.items():
        if t_id in values:
            result_c = key
            break
    return result_c


def match(items, cons):
    num = 0
    
    matched_data = []
    mismatched_keys = []
    
    for res in items:
        if res['prefix'] is not None:
            turtle = res['prefix'] + res['turtle']
        else:
            turtle = res['turtle']
        themes = []

        # issue: some keywords as dct:subject filttered out because of not uri, this part needs to be rewrite

        if 'dct:subject' in turtle:
            terms = rdfSearchDctSub(turtle)
            keys = [term for term in terms if not term[0].startswith('http')]
            themes = [term[0] for term in terms if term[0].startswith('http')]
        
        else:
            keys = rdfSearchKeys(turtle)
            themes = rdfSearchSubThes(turtle)
        
        if len(keys) ==0 & len(themes) == 0: # nothing found, wich should not happen
            # print(f"No keyword found for record {res['identifier']}")
            num += 1
            # print(f"No keyword found for record {res['identifier']}")

        subs_related = []

        # match the keys
        for key in keys:
            sub_key = label_fuzzmatch(cons, key, threshold = 80)
            if sub_key is not None:
                subs_related.append(sub_key)
            else:
                mismatched_keys.append(key)

        # match the themes
        sub_theme = url_match(cons, themes)
        subs_related.extend(sub_theme)

        # get all the matched subject id
        unique_subs_related = list(set(subs_related))

        if len(unique_subs_related) > 0:
            for sub_id in unique_subs_related:
                # Find the corresponding subject by subject_id
                matched_subject = next((sub for sub in cons if sub['identifier'] == sub_id), None)
                if matched_subject:
                    # insertMatch(res['identifier'], res['hash'], sub_id, matched_subject["labels"]["en"][0] )
                    matched_data.append({
                        'record_identifier': res['identifier'],
                        'hash': res['hash'],
                        'concept_identifier': sub_id,
                        'label': matched_subject["labels"]["en"][0]
                    })
    logging.info(f"Total number of records failed to find keywords:: {num}")
    return matched_data, mismatched_keys

def update_termsf(matched_d, cons):
    # this function is to update the terms.csv file when knowledge graph updated
    
    # read terms.csv file:
    with open('keyword-matcher/result/terms.csv', 'r', encoding='utf-8') as f:
        csvreader = csv.DictReader(f)
        terms = [row for row in csvreader]

    with open('keyword-matcher/result/discard_terms.csv', 'r', encoding='utf-8') as f:
        csvreader = csv.DictReader(f)
        discarded_terms = [row for row in csvreader]
    
    discarded_term_ids = [term['identifier'] for term in discarded_terms]

    matched_ids = [item['concept_identifier'] for item in matched_d]
    matched_ids = list(set(matched_ids))
    logging.info(f"Number of matched terms: {len(matched_ids)}")
    
    terms_new = []
    for term in terms:
        # term in terms.csv, also found in matched terms, keep it and update the uri and label
        if term['identifier'] in matched_ids:
            con_l = [con for con in cons if con['identifier'] == term['identifier']]
            if len(con_l) > 0:
                t = con_l[0]
                uri_list = t['relevant_uris']
                uri = uri_list[0] if len(uri_list) > 0 else ''
                terms_new.append({
                    'identifier': term['identifier'], # from original terms.csv
                    'label': t['labels']['en'][0],
                    'uri': uri,
                    'class': term.get('class', '') # from original terms.csv
                })
        else:
            # term in terms.csv, but no found in matched terms. means that this term is not matched with records anymore, need to remove it.
            logging.info(f"Term {term['identifier']} removed from the terms.csv file")
    
    for matched_id in matched_ids:
        # We found new terms not yet in the terms.csv, because kg updated or because new records have new matched. We add them to the terms.csv, but if it is discarded terms we don't add it
        if matched_id not in [term['identifier'] for term in terms_new]: 
            if matched_id not in discarded_term_ids: # if it is not in the discarded terms
                term_l = [con for con in cons if con['identifier'] == matched_id]
                if len(term_l) > 0:
                    term = term_l[0]
                    uri_list = term['relevant_uris']
                    uri = uri_list[0] if len(uri_list) > 0 else ''
                    terms_new.append({
                        'identifier': term['identifier'],
                        'label': term['labels']['en'][0],
                        'uri': uri,
                        'class': ''
                    })
                    logging.info(f"Term {term['identifier']} added to the terms.csv file, needs to assign a class")
    
    with open('keyword-matcher/result/terms.csv', 'w', newline='', encoding='utf-8') as f:
        csvwriter = csv.DictWriter(f, fieldnames=['identifier', 'label', 'uri', 'class'])
        csvwriter.writeheader()
        csvwriter.writerows(terms_new)

    logging.info("terms.csv file updated")
    

def update_outputf(matched_d, cons, mis_keys):
    # the function to update the output files: unmatched_keywords and unmatched_concepts

    # unmatched concepts

    con_id_matched = list(set([item['concept_identifier'] for item in matched_d]))


    unmatched_concepts = [item for item in cons if item['identifier'] not in con_id_matched]
    res_csv = [[item['identifier'], item['labels']['en'][0]]for item in unmatched_concepts]

    with open('keyword-matcher/result/unmatched_concepts.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['identifier', 'label'])
        writer.writerows(res_csv)
    logging.info(f"Unmatched concepts file updated")
    
    # unmatched keywords
    en_keys = [item[0].lower() for item in mis_keys if item[1] == "en"] # lower case
    label_counts = Counter(en_keys)
    sorted_labels = sorted(label_counts.items(), key=lambda x: x[1], reverse=True)
    with open ('keyword-matcher/result/unmatched_keywords.csv', 'w', newline='') as f:
        csvwriter = csv.writer(f)
        csvwriter.writerow(["Label", "Count", "Remarks"])
        for label, count in sorted_labels:
            csvwriter.writerow([label, count, None])
    logging.info(f"Unmatched keywords file updated")

def get_mapping(terms):
    # classes = [item['class'] for item in terms if item['class'] is not None] # can be harcoded but leave it like this for now
    classes = ["soil chemical properties", 
               "soil biological properties", 
               "soil physical properties",
               "soil classification", 
               "soil functions", 
               "soil threats", 
               "soil processes", 
               "soil management",
               "ecosystem services"]
    terms_classes = list(set([item['class'] for item in terms if item['class'] is not None]))
    for c in classes:
        if c not in terms_classes:
            logging.error(f"Class {c} not found in terms.csv file")
            return None
    
    cols = ['identifier'] + [c.replace(" ", "_") for c in classes]
    # create a mapping object
    mapping = {key: [] for key in classes}
    for t in terms:
        if t['class'] is not None:
            if t['class'] in classes:
                c = t['class']
                i = t['identifier']
                mapping[c].append(i)

    c_mapping = {key.replace(" ", "_"): value for key, value in mapping.items()} 
    return c_mapping, cols

def update_keywords_temp(col_values):
    
    sql = '''
    INSERT INTO public.keywords_temp 
    (identifier, 
    soil_threats,
    soil_processes, 
    soil_classification, 
    soil_properties, 
    soil_functions, 
    soil_chemical_properties,
    productivity, 
    contamination, 
    soil_services, 
    soil_physical_properties, 
    ecosystem_services, 
    soil_biological_properties) 
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT(identifier) DO UPDATE 
    SET 
    soil_threats = EXCLUDED.soil_threats,
    soil_processes = EXCLUDED.soil_processes,
    soil_classification = EXCLUDED.soil_classification,
    soil_properties = EXCLUDED.soil_properties,
    soil_functions = EXCLUDED.soil_functions,
    soil_chemical_properties = EXCLUDED.soil_chemical_properties,
    productivity = EXCLUDED.productivity,
    contamination = EXCLUDED.contamination,
    soil_services = EXCLUDED.soil_services,
    soil_physical_properties = EXCLUDED.soil_physical_properties,
    ecosystem_services = EXCLUDED.ecosystem_services,
    soil_biological_properties = EXCLUDED.soil_biological_properties
    '''

    dbQuery(sql, tuple(col_values), hasoutput=False)

def update_tracking(hash_val, time_now):
    
    sql = '''
    INSERT INTO harvest.process_tracking 
    (process_id, hash, last_run)
    VALUES ('keyword', %s, %s)
    ON CONFLICT(hash, process_id) DO UPDATE 
    SET 
    process_id = EXCLUDED.process_id,
    hash = EXCLUDED.hash,
    last_run = EXCLUDED.last_run
    '''

    dbQuery(sql, (hash_val, time_now), hasoutput=False)

def full_process(opt_output: bool):
    start_time = time.time()
    
    load_dotenv()
    
    # find the records that contain keywords
    logging.info("Querying records from the database view")

    sql = '''
    SELECT * FROM harvest.item_contain_keyword;
    '''
    result_items = turple2dict(dbQuery(sql, hasoutput=True))

    logging.info(f"Query completed, find {len(result_items)} records")
    
    logging.info(f"Query execution: {time.time() - start_time:.4f} seconds")

    # get defined Concepts
    start_time = time.time()

    logging.info("Matching records with concepts")
    
    with open("./keyword-matcher/concepts.json", "r") as f:
        subs = json.load(f)
    logging.info("concept.json file loaded")


    matched_data, mis_keys = match(result_items, subs)
    
    # add code here to update terms.csv
    if opt_output is True:
        update_termsf(matched_data, subs)
        update_outputf(matched_data, subs, mis_keys)
    
    terms = read_csv_to_dict('keyword-matcher/result/terms.csv')
    logging.info("terms.csv file loaded")

    logging.info(f"Match records successfully, found {len(matched_data)} matches")
    logging.info(f"Matching execution: {time.time() - start_time:.4f} seconds")
    
    c_mapping, cols = get_mapping(terms)
    
    # for c, con_ids in c_mapping.items():
    #     for con_id in con_ids:
    #         count_term = sum(1 for item in matched_data if item['concept_identifier'] == con_id)
    #         index = c_mapping[c].index(con_id)
    #         c_mapping[c][index] = [con_id, count_term]

    # # save c_mapping into json file
    # with open('keyword-matcher/c_mapping.json', 'w') as f:
    #     json.dump(c_mapping, f, indent=2)

    logging.info("Truncating and inserting data into the keyword_temp table")

    start_time = time.time()

    # first truncate the temp table
    sql = '''
    TRUNCATE TABLE keywords_temp;
    '''
    dbQuery(sql,  hasoutput=False)

    logging.info("Truncated keyword_temp table")

    keys = ['identifier'] + list(c_mapping.keys())

    # Group matches by record_identifier
    records = {}
    for m in matched_data:
        record_id = m["record_identifier"]
        term_id = m["concept_identifier"]
        records.setdefault(record_id, []).append(term_id) # records with unique identifier

    # insert target data to the temp table
    count_row = 0
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
        # insert row here
        insertSQL('keywords_temp', cols, list(dic.values()) )  # order insensitive
        count_row += 1

    logging.info(f"{count_row} rows inserted to the keywords_temp table")
    logging.info(f"Update keywords_temp table execution: {time.time() - start_time:.4f} seconds")
    
    start_time = time.time()
    logging.info("Updating tracking table")
    # update track table
    sql = '''
    DELETE FROM harvest.process_tracking
    WHERE process_id = 'keyword';
    '''
    dbQuery(sql,  hasoutput=False)
    logging.info("Deleted tracking table")

    time_n = datetime.datetime.now()

    for res in result_items:
        insertSQL('harvest.process_tracking', ['process_id', 'hash', 'last_run'], ['keyword', res['hash'], time_n] )  

    logging.info(f"Update tracking table execution: {time.time() - start_time:.4f} seconds")

def batch_process(interval):

    start_time = time.time()

    load_dotenv()
    
    # find the records that contain keywords
    logging.info("Querying records from the database view")

    # the query filter out records that have been processed, can be changed to filter on interval
    sql = '''
    SELECT harvest.item_contain_keyword.* FROM harvest.item_contain_keyword 
    LEFT JOIN 
    (
    SELECT * FROM harvest.process_tracking
    WHERE harvest.process_tracking.process_id = 'keyword'
    ) AS p_keyword
    ON harvest.item_contain_keyword.hash = p_keyword.hash
    WHERE
    p_keyword.hash ISNULL
    '''
    result_items = turple2dict(dbQuery(sql, hasoutput=True))

    if len(result_items) == 0:
        logging.info("All records have been processed for the keyword-matcher")
        return

    logging.info(f"Query completed, find {len(result_items)} new records")
    
    logging.info(f"Query execution: {time.time() - start_time:.4f} seconds")

    # get defined Concepts
    start_time = time.time()

    with open("./keyword-matcher/concepts.json", "r") as f:
        subs = json.load(f)
    logging.info("concept.json file loaded")

    terms = read_csv_to_dict('keyword-matcher/result/terms.csv')

    logging.info("terms.csv file loaded")
    c_mapping, cols = get_mapping(terms)

    logging.info("Matching records with concepts")

    matched_data, mis_keys = match(result_items, subs)
    # add code here to update terms.csv

    logging.info(f"Match completed, found {len(matched_data)} matches")
    logging.info(f"Match execution: {time.time() - start_time:.4f} seconds")

    start_time = time.time()

    keys = ['identifier'] + list(c_mapping.keys())

    # Group matches by record_identifier
    records = {}
    for m in matched_data:
        record_id = m["record_identifier"]
        term_id = m["concept_identifier"]
        records.setdefault(record_id, []).append(term_id) # records with unique identifier

    # insert target data to the temp table

    logging.info("Inserting or updating data into the keyword_temp table")
    count_row = 0
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
        # insert or update row here
        update_keywords_temp(list(dic.values()) ) 
        count_row += 1
    
    logging.info(f"Keywords_temp table update execution: {time.time() - start_time:.4f} seconds")
    logging.info(f"{count_row} rows updated to the keywords_temp table")

    logging.info("Updating tracking table")

    start_time = time.time()

    time_n = datetime.datetime.now()
    for res in result_items:
        update_tracking(res['hash'], time_n)

    logging.info(f"Tracking table execution: {time.time() - start_time:.4f} seconds")


def main(argv):

    arg_help = "dummy help message".format(argv[0])
    arg_batch = False
    arg_output = False
    arg_time = 0
    opts, args = getopt.getopt(argv[1:], "hb:t:o:", ["help", "batch=", "time=", "output="])

    logging.basicConfig(level=logging.INFO, format = '%(message)s')

    for opt, arg in opts:
        if opt in ("-h", "--help"):
            logging.info(arg_help)
            sys.exit()
        elif opt in ("-b", "--batch"):
            arg_batch = arg.lower() == 'true'
        elif opt in ("-o", "--option"):
            arg_output = arg.lower() == 'true'
        elif opt in ("-t", "--time"):
            arg_time = arg
    
    if arg_batch is False:
        logging.info("Start full process running")
        full_process(arg_output)
    else:
        if arg_output is True:
            logging.error("batch process does not support updating output")
            sys.exit()
        logging.info("Start batch process running")
        batch_process(arg_time)


if __name__ == "__main__":
    main(sys.argv)