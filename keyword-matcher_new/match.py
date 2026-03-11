# match the records and dump result to keyword_temp

# input: concept.json, database: metadata.subjects
# output: database: metadata.keywords_match

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



def turple2dict(rows): # transform a query result from turple to dict
    col_names = ['id', 'uri', 'label']  
    return [dict(zip(col_names, row)) for row in rows]


def formatString(input_string): # remove prefix numbers from a label
    stripped_string = input_string.lstrip()
    
    # Check if the stripped string starts with a number
    if stripped_string and stripped_string[0].isdigit():
        # Remove leading numbers
        return stripped_string.lstrip('0123456789')
    
    return input_string.lstrip()


def label_fuzzmatch(cons, key_value, threshold=80):
    
    key_value = formatString(key_value) # remove the number prefix
    best_match_concept = None
    
    flag = 0

    for c in cons:

        c_label_dict = c['labels']
        
        c_all_labels = [label for labels in c_label_dict.values() for label in labels]
        
        con_flag = 0
        # get the highest matched value
        for lab in c_all_labels:
            matched_ratio = fuzz.ratio(key_value.lower(), lab.lower()) # if case sensitive. fuzz.ratio ("water", "water") = 80, short words can have a low match score
            if matched_ratio > con_flag:
                con_flag = matched_ratio
        
        # get the highest matched subject and the matched value
        if con_flag > flag:
            flag = con_flag
            best_match_concept = c

        
    if flag >= threshold:
        # apply the match
        return best_match_concept, flag
    else:
        return None, 0


def url_match(cons, sub_url):
    """
    Match a subject URL against concept URIs.
    
    Args:
        cons: List of concept objects from concepts.json
        sub_url: The subject URI to match against concept URIs
    
    Returns:
        The matched concept object if found, None otherwise
    """
    matched_con = None

    # Iterate through all concepts to find a matching URI
    for c in cons:
        con_urls = c['uris']
        # Check if the subject URL exists in this concept's URIs
        if sub_url in con_urls:
            matched_con = c
            break  # Return first match found

    return matched_con



def get_label(data, identifier):
    for item in data:
        if item["identifier"] == identifier:
            return item["label"].lower() # transform to lower case
    return None  # Return None if not found


def match_res_sub(items, cons):
    """
    items: result items from database (subjects)
    cons: concept object from concept.json
    
    """

    matched_dict_list = []
    
    for res in items:

        matched_dict = {
        'sub_identifier': res.get('id'),
        'vocab_identifier': None,
        'vocab_label': None,
        'fuzzymatch_score': None
        }

        sub_uri = res.get('uri')
        sub_label = res.get('label')

        if sub_uri is not None:
            # subject has uri, use uri for matching
            matched_con = url_match(cons, sub_uri)
            if matched_con is None: # can't find a match
                matched_dict_list.append(matched_dict)
            else: # find a match
                matched_dict['vocab_identifier'] = matched_con.get('identifier')
                matched_dict['vocab_label'] = matched_con.get('labels', {}).get('en', [])[0]
                matched_dict_list.append(matched_dict)
        
        elif sub_label is not None:
            # subject has no uri but has label. use label for matching
            con_, fuzzy_score = label_fuzzmatch(cons, sub_label, threshold = 80)
            if con_ is None: # can't find a match
                matched_dict_list.append(matched_dict)
            else: # find a match
                matched_dict['vocab_identifier'] = con_.get('identifier')
                matched_dict['vocab_label'] = con_.get('labels', {}).get('en', [])[0]
                matched_dict['fuzzymatch_score'] = fuzzy_score
                matched_dict_list.append(matched_dict)

    
    return matched_dict_list



def batch_process(interval):

    start_time = time.time()

    load_dotenv()
    
    # find the records that contain keywords
    logging.info("Querying records from the database view")

    # the query filter out subjects that have been processed
    sql = '''
    SELECT id, uri, label FROM metadata.subjects s
    WHERE NOT EXISTS (
    SELECT 1 FROM metadata.keyword_match km 
    WHERE km.subject_id = s.id
    )
    '''
    result_items = turple2dict(dbQuery(sql, hasoutput=True))

    if len(result_items) == 0:
        logging.info("All records have been processed for the keyword-matcher")
        return

    logging.info(f"Query completed, find {len(result_items)} new records")
    
    logging.info(f"Query execution: {time.time() - start_time:.4f} seconds")

    # get defined Concepts
    start_time = time.time()

    with open("./keyword-matcher_new/concepts.json", "r") as f:
        cons = json.load(f)
    logging.info("concept.json file loaded")

    logging.info("Matching records with concepts")

    matched_data = match_res_sub(result_items, cons)

    logging.info(f"Match completed, match execution: {time.time() - start_time:.4f} seconds")
    start_time = time.time()

    logging.info("Inserting matched data to database table")

    process_datetime = datetime.datetime.fromtimestamp(start_time, tz=datetime.timezone.utc)
    
    matched_data_tuples = [
        (item['sub_identifier'], item['vocab_identifier'], item['vocab_label'], 
         item['fuzzymatch_score'], process_datetime)
        for item in matched_data
    ]
    insertBulkSQL('metadata.keyword_match', 
                  ['subject_id', 'vocab_id', 'vocab_label', 'fuzzymatch_score', 'process_time'], 
                  matched_data_tuples)

    logging.info(f"Database insert completed, execution: {time.time() - start_time:.4f} seconds")


def main(argv):

    arg_help = "dummy help message".format(argv[0])
    arg_output = False
    arg_time = 0
    opts, args = getopt.getopt(argv[1:], "h:t:o:", ["help", "time=", "output="])

    logging.basicConfig(level=logging.INFO, format = '%(message)s')

    for opt, arg in opts:
        if opt in ("-h", "--help"):
            logging.info(arg_help)
            sys.exit()
        elif opt in ("-o", "--option"):
            arg_output = arg.lower() == 'true'
        elif opt in ("-t", "--time"):
            arg_time = arg
    
    logging.info("Start batch process running")
    batch_process(arg_time)


if __name__ == "__main__":
    main(sys.argv)