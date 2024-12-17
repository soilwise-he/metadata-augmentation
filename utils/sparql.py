from SPARQLWrapper import SPARQLWrapper, JSON
from rdflib import Graph


def sparqlRemote(endpoint, query):
    try:
        # Initialize the SPARQLWrapper with the endpoint URL
        sparql = SPARQLWrapper(endpoint)
        sparql.setQuery(query)
        sparql.setReturnFormat(JSON)  # Set the return format to JSON
        
        # Execute the query and fetch results
        results = sparql.query().convert()
        return results['results']['bindings']  # Return the bindings from the response
    except Exception as e:
        print(f"An error occurred: {e}")
        return None
    

def sparqlLocal(rdf, query, rdf_format):
    # exceute sparql query for local rdf
    # for ttl file, rdf = file path, rdf_format = "ttl"
    # for turtle string, rdf = rdf string, rdf_format = "turtle"
    try:
        g = Graph()
        g.parse(rdf, format = rdf_format)
        results = g.query(query
            
        )
        formatted_results = [
            {str(var): str(row[var]) for var in results.vars if row[var] is not None}
            for row in results
        ]
        return formatted_results
    
    except Exception as e:
        print(f"An error occurred: {e}")
        return None
    
# path = "./keyword-matcher/soil_health_KG.ttl"
# query = '''
#         prefix skos:  <http://www.w3.org/2004/02/skos/core#> 
#         SELECT *
#         WHERE {
#         ?p skos:exactMatch ?o
#         }  
# '''

# g = Graph()
# res = sparqlLocal(path, query, "ttl")
# print(res)