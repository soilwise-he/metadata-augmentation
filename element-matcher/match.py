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


def turple2dict(rows, col_names): # transform a query result from turple to dict 
    return [dict(zip(col_names, row)) for row in rows]


def csv2mapping(csv_file):
    mapping = {}
    with open(csv_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:   
            source_label = row['source_label']
            if row['target_label'] == '':
                target_label = None
            else:
                target_label = row['target_label']
            mapping[source_label] = target_label
    return mapping

def get_element_type(turtle):
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

def get_element_license(turtle):
    try:
        g = Graph()
        g.parse(data=turtle, format="turtle")

        query = '''
        prefix dcterms: <http://purl.org/dc/terms/> 
        select ?license
        {
        ?s dcterms:license ?license.
        FILTER (isIRI(?license) || isLiteral(?license))
        }
        ''' 
        results = g.query(query)
        for row in results:
            return str(row['license'])
        # return [str(row['type']) for row in results]

    except Exception as e:
        logging.error(e)
        return None

def get_element(item):

    if item['turtle'] is None:
        return None
    
    if item['prefix'] is not None:
        turtle = item['prefix'] + item['turtle']
    else:
        turtle = item['turtle']

    ele_type = get_element_type(turtle)
    # ele_license = get_element_license(turtle)
    # return ele_license  
    return ele_type

def match_elements():
    start_time = time.time()

    load_dotenv()
    
    # find the records that contain keywords
    logging.info("Querying records from the database table")

    sql = '''
    SELECT i.identifier,i.hash,i.uri,i.turtle,sources.turtle_prefix 
    FROM harvest.items i LEFT JOIN harvest.sources ON i.source = sources.name::text
    WHERE i.insert_date = (( SELECT max(t.insert_date) AS max FROM harvest.items t WHERE t.identifier = i.identifier));
    '''
    result_items = turple2dict(dbQuery(sql, hasoutput=True), ['identifier', 'hash', 'uri', 'turtle', 'prefix'] )

    logging.info(f"Query completed, find {len(result_items)} records")
    
    logging.info(f"Query execution: {time.time() - start_time:.2f} seconds")
    
    logging.info("Quering elements from turtles")

    start_time = time.time()

    for item in result_items:
        item['type'] = get_element(item)

    logging.info(f"Turtle query: {(time.time() - start_time)/60:.2f} minutes")
        
    logging.info("Matching elements and inserting to the augmentation table")
    # get mapping from csv
    start_time = time.time()

    m_type = csv2mapping('element-matcher/mapping/type.csv')

    dbQuery('TRUNCATE table harvest.augmentation', hasoutput=False)
    
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
            item['type'] = None
        insertSQL('harvest.augmentation', ['identifier', 'type'], [item['identifier'], item['type']] )
    
    logging.info(f"Inserting execution: {(time.time() - start_time)/60:.2f} minutes")
    
    return

def match_types(result_items, m_type, target_type_list):
    for item in result_items:
        if item['type'] is None:
            continue
        source_type = item['type']
        target_type = m_type.get(source_type, 'no match')
        if target_type != 'no match': # find a match
            item['type'] = target_type
        elif source_type in target_type_list: # already transformed
            item['type'] = source_type 
        else: # can't find a match
            logging.info(f"Type {source_type} not found in the mapping file")
            item['type'] = None
        insertSQL('harvest.augmentation', ['identifier', 'value', 'element_type'], [item['identifier'], item['type'], 'type'] )

def match_elements_precords():
    start_time = time.time()

    load_dotenv()
    
    # find the records that contain keywords
    logging.info("Querying records from the database table")

    sql = '''
    SELECT identifier, type FROM public.records2;
    '''
    result_items = turple2dict(dbQuery(sql, hasoutput=True), ['identifier', 'type'] )

    logging.info(f"Query completed, find {len(result_items)} records")
    
    logging.info(f"Query execution: {time.time() - start_time:.2f} seconds")
    
    # get mapping from csv
    m_type = csv2mapping('element-matcher/mapping/type.csv')
    target_type_list = list(set(i for i in m_type.values() if i is not None))

    start_time = time.time()
    logging.info("Matching elements and inserting type to the augmentation table")

    dbQuery('TRUNCATE table harvest.augmentation', hasoutput=False)
    
    # match
    match_types(result_items, m_type, target_type_list)
    
    logging.info(f"Inserting type execution: {(time.time() - start_time)/60:.2f} minutes")

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
        match_elements_precords()
    else:
        logging.info("Start batch process running")
        return
        batch_process(arg_time)

if __name__ == "__main__":
    main(sys.argv)