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

# def match_types(result_items):
#     """
    
    
    
#     """
#     # get mapping from csv
#     m_type = csv2mapping('element-matcher/mapping/type.csv')
#     target_type_list = list(set(i for i in m_type.values() if i is not None))

#     missing_types = []
#     res_type = []
#     for item in result_items:
#         if item['type'] is None:
#             continue
#         source_type = item['type']
#         target_type = m_type.get(source_type, 'no match')
#         if target_type != 'no match': # find a match
#             item['type'] = target_type
#         elif source_type in target_type_list: # already transformed
#             item['type'] = source_type 
#         elif source_type not in missing_types: # can't find a match, log once
#             logging.info(f"Type {source_type} from the record {item['identifier']} not found in the mapping file")
#             missing_types.append(source_type)
#             item['type'] = None
#         else:
#             item['type'] = None

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
    target_type_list = set(i for i in m_type.values() if i is not None)  # Use set for O(1) lookup
    
    # Initialize output and tracking
    res_type = []
    missing_types = set()  # Use set instead of list for O(1) lookup

    process_datetime = datetime.datetime.fromtimestamp(process_time, tz=datetime.timezone.utc)
    
    for item in result_items:
        # Case 1: type is None
        if item['type'] is None:
            target_type = 'Unknown'
        else:    
            source_type = item['type']
            target_type = m_type.get(source_type, 'no match')
        
        if target_type != 'no match':
            # Case 2: Found a match
            final_type = target_type
        elif source_type in target_type_list:
            # Case 3: Already in target format
            final_type = source_type
        else:
            # Case 4: No match found, skip it
            if source_type not in missing_types:
                logging.info(f"Type '{source_type}' from record '{item['identifier']}' not found in mapping file")
                missing_types.add(source_type)
            final_type = None
        
        # Only add successfully mapped items to output
        if final_type is not None:
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
    SELECT r.identifier, r.type, r.license from metadata.records r
    where r.identifier not in
    (select record_id from metadata.augments
    where process ilike 'element-matcher');
    '''
    result_items = turple2dict(dbQuery(sql, hasoutput=True), ['identifier', 'type', 'license'] ) # Currently include here type and license

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


    for row in result_type:
        # add the insertSql function here to insert values
        insertSQL('metadata.augments', ['record_id', 'property', 'value', 'process', 'date'], row)

    logging.info(f"Insert type completed, {len(result_type)} rows inserted")

    logging.info(f"Insert type execution: {(time.time() - start_time)/60:.2f} minutes")
    start_time = time.time()

    # later to add other EM, license, ...

    return
    
    # match
    match_types(result_items, m_type, target_type_list)
    
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