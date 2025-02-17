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


def csv2mapping(csv_file):
    mapping = {}
    with open(csv_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:   
            source_label = row['source_label']
            target_label = row['target_label']
            mapping[source_label] = target_label
    return mapping



def get_element(item):

    if item['turtle'] is None:
        return None
    
    if item['prefix'] is not None:
        turtle = item['prefix'] + item['turtle']
    else:
        turtle = item['turtle']

    try:
        g = Graph()
        g.parse(data=turtle, format="turtle")

        query = '''
        prefix dct: <http://purl.org/dc/terms/>
        SELECT ?p
        WHERE {
            ?p dct:type 'journalpaper'
        }
        '''
        results = g.query(query)
        if len(results) > 0:
            return 'journalpaper'

        query = '''
        prefix dct: <http://purl.org/dc/terms/>
        SELECT ?type
        WHERE {
            ?p dct:identifier ?identifier;
               dct:type ?type
            FILTER (?type != <http://inspire.ec.europa.eu/glossary/SpatialReferenceSystem>) 
        }
        ''' 
        results = g.query(query)
        for row in results:
            return str(row['type'])
        # return [str(row['type']) for row in results]

    except Exception as e:
        logging.error(e)
        return None
       

def match_elements():
    start_time = time.time()

    load_dotenv()
    
    # find the records that contain keywords
    logging.info("Querying records from the database table")

    sql = '''
    SELECT items.identifier,items.hash,items.uri,items.turtle,sources.turtle_prefix
    FROM harvest.items LEFT JOIN harvest.sources ON items.source = sources.name::text;
    '''
    result_items = turple2dict(dbQuery(sql, hasoutput=True))

    logging.info(f"Query completed, find {len(result_items)} records")
    
    logging.info(f"Query execution: {time.time() - start_time:.4f} seconds")

    start_time = time.time()

    # Query elements from the record turtle
    logging.info("Querying elements from the record turtle")


    for item in result_items:
        item['type'] = get_element(item)
    logging.info(f"Sparql query execution: {time.time() - start_time:.4f} seconds")

    # get mapping from csv
    start_time = time.time()

    m_type = csv2mapping('element-matcher/mapping/type.csv')
    
    # match
    for item in result_items:
        if item['type'] is None:
            continue
        source_type = item['type']
        target_type = m_type.get(source_type, 'no match')
        if target_type != 'no match':
            item['type'] = target_type
        else:
            logging.info(f"Type {source_type} not found in the mapping file")
            item['type'] = 'other'
    return

    logging.info("Matching records with concepts")
    
    with open("./keyword-matcher/concepts.json", "r") as f:
        subs = json.load(f)
    logging.info("concept.json file loaded")

    terms = read_csv_to_dict('keyword-matcher/result/terms.csv')
    logging.info("terms.csv file loaded")

    matched_data = match(result_items, subs)
    # add code here to update terms.csv

    logging.info(f"Match records successfully, found {len(matched_data)} matches")
    logging.info(f"Matching execution: {time.time() - start_time:.4f} seconds")

    logging.info("Truncating and inserting data into the keyword_temp table")

    start_time = time.time()

    c_mapping, cols = get_mapping(terms)

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
        insertSQL('keywords_temp', cols, list(dic.values()) )  
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



def main(argv):

    arg_help = "dummy help message".format(argv[0])
    arg_batch = False
    arg_time = 0
    opts, args = getopt.getopt(argv[1:], "hb:t:", ["help", "batch=", "time="])

    logging.basicConfig(level=logging.INFO, format = '%(message)s')

    for opt, arg in opts:
        if opt in ("-h", "--help"):
            print(arg_help)
            sys.exit()
        elif opt in ("-b", "--batch"):
            arg_batch = arg.lower() == 'true'
        elif opt in ("-t", "--time"):
            arg_time = arg
    
    if arg_batch is False:
        match_elements()
    else:
        logging.info("Start batch process running")
        return
        batch_process(arg_time)



if __name__ == "__main__":
    main(sys.argv)