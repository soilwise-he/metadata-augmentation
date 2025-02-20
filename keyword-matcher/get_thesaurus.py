# get concepts from soil-health knowledge graph and :
# attach the exactMatch Agrovoc and iso urls;
# attach labels (en, fr, de, it, es)

import sys
sys.path.append('utils')
from sparql import sparqlLocal, sparqlRemote
import json
from collections import Counter


def searchAgro(ag_uri):
    # query the ArgroVoc by an ArgroVoc uri, to get prelabels and altLabels
    ## ag_uri = "http://aims.fao.org/aos/agrovoc/c_40bce4ff"
    endpoint = "https://agrovoc.fao.org/sparql"
    sparql = f'''
    PREFIX skos: <http://www.w3.org/2004/02/skos/core#>

    SELECT DISTINCT ?label
    WHERE {{
        {{ <{ag_uri}> skos:prefLabel ?label }} UNION 
        {{ <{ag_uri}> skos:altLabel ?label }}  
    }}
    '''
    res = sparqlRemote(endpoint, sparql)
    return res

def searchIso(iso_uri):
    # query the iso11074 by an iso uri, to get prelabels and altLabels
    iso_path = "./keyword-matcher/ISO11074.ttl"
    sparql = f'''
    PREFIX skos: <http://www.w3.org/2004/02/skos/core#>

    SELECT DISTINCT ?label ?lang
    WHERE {{
        {{ <{iso_uri}> skos:prefLabel ?label .
           BIND(LANG(?label) AS ?lang)
        }} UNION 
        {{ <{iso_uri}> skos:altLabel ?label .
         BIND(LANG(?label) AS ?lang) }}  
    }}
    '''
    res = sparqlLocal(iso_path, sparql,"ttl")
    return res

# print(searchIso("https://data.geoscience.earth/ncl/ISO11074/7.1.5"))


def processLabels(res, mode, langs):
    """
    Process labels from a sparql result to select certain languages and re-format the labels
    
    Args:
    - res (list): sparql result of querying for labels
    - mode (int): query mode, 1: remote query, 2: local query
    - langs (list): selected languages
    - original_lab (string): exsisting english label from the thesaurus (KG)
    
    Returns:
    - formatted_labs: structured selction of labels by languages
    """
    # initialize the formatted_labs
    formatted_labs = {lang: [] for lang in langs}
    
    match mode:
        case 1: # remote query res
            for item in res:
                # Extract language and value from the nested dictionary
                lang = item['label'].get('xml:lang', '')
                value = item['label'].get('value', '')
                # If the language is in the requested languages and not already in the list
                if lang in langs and value:
                    formatted_labs[lang].append(value)
        
        case 2: # local query res
            for item in res:
                # Extract language and label 
                lang = item.get('lang', '')
                label = item.get('label', '')
                # If the language is in the requested languages and not already in the list
                if lang in langs and label:
                    formatted_labs[lang].append(label)
        
        case _:
            # Handle unexpected mode
            raise ValueError(f"Unsupported query mode: {mode}")
    
    # Remove any languages with empty lists
    formatted_labs = {k: v for k, v in formatted_labs.items() if v}
    
    return formatted_labs

def mergeLabels(dict_a, dict_b):
    """
    merge label dictionaries
    dict_a: original lab dict
    dict_b: lab dict to be merged
    """
    merged_dict = dict_a.copy()
    
    # Iterate through languages in dict_b
    for lang, labels in dict_b.items():
        # If the language doesn't exist in merged_dict, add it
        if lang not in merged_dict:
            merged_dict[lang] = []
        
        # Extend labels and remove duplicates
        merged_dict[lang].extend(labels)
        merged_dict[lang] = list(dict.fromkeys(merged_dict[lang]))
    return merged_dict

def remove_redun_cons(concepts):
    ids = [con['identifier'] for con in concepts]# many concepts are repeated, remove the repeated ones
    counts = Counter(ids)
    redun_ids = [k for k, v in counts.items() if v > 1]
    for reid in redun_ids:
        redun_cons = [con for con in concepts if con['identifier'] == reid]
        if len(redun_cons) == 2: # now only consider 2 repeated concepts
           redun_cons[0]['relevant_uris'].extend(redun_cons[1]['relevant_uris']) # merge the relevant_uris
           redun_cons[0]['labels'] = mergeLabels(redun_cons[0]['labels'], redun_cons[1]['labels']) # merge the labels
           concepts.remove(redun_cons[1])
        else:
            print(f"more than 2 repeated concepts: {reid}")
    print(f"length of concepts after removing redun: {len(concepts)}")
    return concepts

def main():
    # Change below to a remote sparql query when the KG updated to triple store
    kg_path = "./keyword-matcher/soil_health_KG.ttl"

    # ! needs to be modified
    # assume only 1 exact match or close match
    # assume only 1 prefLabel

    query = '''
    prefix skos: <http://www.w3.org/2004/02/skos/core#>
    SELECT DISTINCT ?concept ?label ?exact_match_uri ?close_match_uri
    WHERE {
        ?concept a skos:Concept;
                skos:prefLabel ?label.
        OPTIONAL { ?concept skos:exactMatch ?exact_match_uri }
        OPTIONAL { ?concept skos:closeMatch ?close_match_uri }
    }
    '''    

    kg_concs = sparqlLocal(kg_path, query, "ttl")
    # issue: a concept can have multiple labels
    langs = ["en", "fr", "de", "it", "es", "nl"]

    formatted_cons = []

    for c in kg_concs: # <= 1 exact match, <= 1 close match
        
        # initialize the concept dict
        con_dict = {
            "identifier": c.get('concept', ''),
            "relevant_uris": [],  
            "labels": {
                "en": [c.get('label', '')] if c.get('label') else []
            }
        }

        if c.get("exact_match_uri"):# if exact match exsists
            uri = c["exact_match_uri"]
            con_dict["relevant_uris"].append(uri)
            if uri.startswith("http://aims.fao.org/aos/agrovoc"):
                res_agro = searchAgro(uri)
                if res_agro is not None:
                    lab_dict = processLabels(res_agro, 1, langs)
                    con_dict["labels"] = mergeLabels(con_dict["labels"], lab_dict)
                    
                
            elif uri.startswith("https://data.geoscience.earth/ncl/ISO11074"):
                res_iso = searchIso(uri)
                if res_iso is not None:
                    lab_dict = processLabels(res_iso, 2, langs)
                    con_dict["labels"] = mergeLabels(con_dict["labels"], lab_dict)
            # if uri not from agro or iso, do nothing

        if c.get("close_match_uri"):# if close match exsists
            uri = c["close_match_uri"]
            con_dict["relevant_uris"].append(uri)
            if uri.startswith("http://aims.fao.org/aos/agrovoc"):
                res_agro = searchAgro(uri)
                if res_agro is not None:
                    lab_dict = processLabels(res_agro, 1, langs)
                    con_dict["labels"] = mergeLabels(con_dict["labels"], lab_dict)
                            
            elif uri.startwith("https://data.geoscience.earth/ncl/ISO11074"):
                res_iso = searchIso(uri)
                if res_iso is not None:
                    lab_dict = processLabels(res_iso, 2, langs)
                    con_dict["labels"] = mergeLabels(con_dict["labels"], lab_dict)
        
        formatted_cons.append(con_dict) 

    print(f"Total concepts: {len(formatted_cons)}")

    concepts_f = remove_redun_cons(formatted_cons)

    with open("./keyword-matcher/concepts.json", "w", encoding='utf-8') as json_file:
        json.dump(concepts_f, json_file, ensure_ascii=False, indent=2) 

    print("concepts.json updated")


if __name__ == "__main__":
    main()






        
