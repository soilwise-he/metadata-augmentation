# match the records and dump result to keyword_temp

# input: concept.json, database..., terms.csv
# output: database: public.keyword_temp

import time

import datetime

import sys

sys.path.append('utils')

from database import dbQuery, insertSQL, insertBulkSQL

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
            mapping[source_label.lower()] = target_label
    return mapping



def match_types(result_items, mapping_file, process_time):
    """
    Match element types using CSV mapping and prepare data for database insert.
    
    Args:
        result_items: List of dicts with 'identifier' and 'type' keys
        mapping_file: Path of the mapping csv file
        process_time: Time of processing
        
    Returns:
        List of tuples ready for insert: (identifier, mapped_type, element_type, process)
    """
    # Load mapping from CSV
    m_type = csv2mapping(mapping_file)

    # Initialize output and tracking
    res_type = []
    missing_types = set()  # Use set instead of list for O(1) lookup

    process_datetime = datetime.datetime.fromtimestamp(process_time, tz=datetime.timezone.utc)
    
    for item in result_items:
        if item['type'] is None or item['type'] == '':
            final_type = None
        else:
            source_type = item['type']
            target_type = m_type.get(source_type.lower())
            if target_type is not None:
                final_type = target_type
            else:
                if source_type not in missing_types:
                    logging.info(f"Type '{source_type}' from record '{item['identifier']}' not found in mapping file")
                    missing_types.add(source_type)
                final_type = None

        res_type.append((
            item['identifier'],
            'type',
            final_type,
            'element-matcher',
            process_datetime
        ))
    
    # Log summary statistics
    logging.info(f"Successfully mapped {len(res_type)} record types")
    if missing_types:
        logging.info(f"Found {len(missing_types)} unmapped type values: {sorted(missing_types)}")
    
    return res_type


def match_langs(result_items, mapping_file, process_time):
    """
    Match element languages using CSV mapping and prepare data for database insert.
    
    Args:
        result_items: List of dicts with 'identifier' and 'lang' keys
        mapping_file: Path of the mapping csv file
        process_time: Time of processing
        
    Returns:
        List of tuples ready for insert: (identifier, mapped_lang, element_type, process)
    """
    # Load mapping from CSV
    m_lang = csv2mapping(mapping_file)
    
    # Initialize output and tracking
    res_lang = []
    missing_langs = set()  # Use set instead of list for O(1) lookup

    process_datetime = datetime.datetime.fromtimestamp(process_time, tz=datetime.timezone.utc)
    
    for item in result_items:
        if item['lang'] is None or item['lang'] == '':
            final_lang = None
        else:
            source_lang = item['lang']
            target_lang = m_lang.get(source_lang.lower())
            if target_lang is not None:
                final_lang = target_lang
            else:
                if source_lang not in missing_langs:
                    logging.info(f"Language '{source_lang}' from record '{item['identifier']}' not found in mapping file")
                    missing_langs.add(source_lang)
                final_lang = None

        res_lang.append((
            item['identifier'],
            'language',
            final_lang,
            'element-matcher',
            process_datetime
        ))

    if missing_langs:
        logging.info(f"Found {len(missing_langs)} unmapped language values: {sorted(missing_langs)}")
    
    
    return res_lang
        

def match_elements_precords():
    """
    Match elements for all the element types.
    All database transactions in this function.

    """
    start_time = time.time()

    load_dotenv()
    
    # Get records which have not been processed by element-matcher
    logging.info("Querying records from the database table")

    sql = '''
    SELECT r.identifier, r.type, r.license, r.language from metadata.records r
    where r.identifier not in
    (select record_id from metadata.augments
    where process ilike 'element-matcher');
    '''
    result_items = turple2dict(dbQuery(sql, hasoutput=True), ['identifier', 'type', 'license', 'lang'] ) # Currently include here type and license
    # Deplicate (temperory solution)
    seen_identifiers = set()
    deduplicated_items = []
    duplicate_count = 0
    
    for item in result_items:
        if item['identifier'] not in seen_identifiers:
            seen_identifiers.add(item['identifier'])
            deduplicated_items.append(item)
        else:
            duplicate_count += 1
    
    if duplicate_count > 0:
        logging.warning(f"Found and removed {duplicate_count} duplicate identifiers, keeping first occurrence")
    
    result_items = deduplicated_items

    logging.info(f"Query completed, find {len(result_items)} unique records")
    
    logging.info(f"Query execution: {time.time() - start_time:.2f} seconds")
    start_time = time.time()

    # -----------------Element: type-------------------------------
    
    logging.info('Matching type')
    result_type = match_types(result_items, 'element-matcher/mapping/type_new.csv', start_time)

    logging.info(f"Matching type completed, execution: {time.time() - start_time:.2f} seconds")
    start_time = time.time()

    logging.info('Inserting type result into the aguments table')

    insertBulkSQL('metadata.augments',['record_id', 'property', 'value', 'process', 'date'], result_type )

    logging.info(f"Insert type completed, {len(result_type)} rows inserted")

    logging.info(f"Insert type execution: {time.time() - start_time:.2f} seconds")
    start_time = time.time()

    # ---------------------Element: language------------------------------

    logging.info('Matching language')
    result_lang = match_langs(result_items, 'element-matcher/mapping/lang.csv', start_time)
    
    logging.info(f"Matching language completed, execution: {time.time() - start_time:.2f} seconds")
    start_time = time.time()

    logging.info('Inserting language result into the aguments table')

    insertBulkSQL('metadata.augments',['record_id', 'property', 'value', 'process', 'date'], result_lang )

    logging.info(f"Insert language completed, {len(result_lang)} rows inserted")

    logging.info(f"Insert language execution: {time.time() - start_time:.2f} seconds")
    start_time = time.time()

    return
    
    # match
    
    logging.info(f"Inserting type execution: {(time.time() - start_time)/60:.2f} minutes")


def main(argv):

    arg_help = "dummy help message".format(argv[0])
    arg_time = 0
    opts, args = getopt.getopt(argv[1:], "ht:", ["help", "time="])

    logging.basicConfig(level=logging.INFO, format = '%(message)s')

    for opt, arg in opts:
        if opt in ("-h", "--help"):
            print(arg_help)
            sys.exit()
        elif opt in ("-t", "--time"):
            arg_time = arg
    
    match_elements_precords()


if __name__ == "__main__":
    main(sys.argv)