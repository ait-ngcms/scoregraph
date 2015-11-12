#!/usr/bin/env python
"""
Script for DBPedia connection.

Invocation:
$ python dbpedia_helper.py query
"""

import argparse
import sys

# HTTP connection
import requests

# helper for SPARQL queries
from SPARQLWrapper import SPARQLWrapper, JSON


DEXTER_API_URI            = "http://dexterdemo.isti.cnr.it:8080/dexter-webapp/api/rest/spot-entities"
DEXTER_API_DBPEDIA_ID_URI = "http://dexterdemo.isti.cnr.it:8080/dexter-webapp/api/rest/get-desc?"

AUTHOR_POS = 1


# sample link http://dexterdemo.isti.cnr.it:8080/dexter-webapp/api/rest/get-desc?id=49109&title-only=false
def find_dbpedia_id(query):
    payload = {'id': query,
               'title-only': 'false'}
    r = requests.get(DEXTER_API_DBPEDIA_ID_URI, params=payload)
    print 'find DBPedia ID status code', r.status_code, 'query', query
    if(r.status_code != 200):
        print("FAILURE: Request", r.url, "failed")
        return None
    result = r.json()
    return result.get('url')


def analyze_titles_by_dbpedia(query_list):

    # spot entities - detects all the mentions in DBPedia that could refer to an entity in the text
    print("\tSearching Dexter for author, subject and title.")

    #dbpedia_items = None
    for query in query_list[1:3]:
        print 'query:', query
        dbpedia_items = find_dbpedia_items(query)
        if(dbpedia_items is None):
            print("0 results.")
            continue
        else:
            print(len(dbpedia_items), "results.")

    # get description for each dexter ID - get DBPedia ID
    sparql = SPARQLWrapper("http://dbpedia.org/sparql")
    for key, value in dbpedia_items.iteritems():
        dbpedia_id = find_dbpedia_id(key)
        print 'DBPedia ID', dbpedia_id
        id_core = dbpedia_id.split("/")[-1]
        print 'id core', id_core
        query_string = \
            "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> " \
            + "SELECT ?label ?type ?comment " \
            + "WHERE { " \
            + "<http://dbpedia.org/resource/" + id_core + "> rdfs:label ?label . " \
            + "<http://dbpedia.org/resource/" + id_core + "> rdf:type ?type . " \
            + "<http://dbpedia.org/resource/" + id_core + "> rdfs:comment ?comment}"
        print 'DBPedia query string: ', query_string

        # use SPARQL query with DBPedia ID to extract more information
        sparql.setQuery(query_string)
        sparql.setReturnFormat(JSON)
        results = sparql.query().convert()

        for result in results["results"]["bindings"]:
            print('label: ', result["label"]["value"])
            print('type: ', result["type"]["value"])
            print('comment: ', result["comment"]["value"])


# this method queries Dexter - detects all the mentions in DBPedia
# that could refer to an entity in the text. column is a number of column
# in the CSV file
def query_dexter(query_list, column):

    for query in query_list[1:]: # without header
        print 'query:', query
        print query[column:column + 1]
        dbpedia_items = find_dbpedia_items(query[column:column])
        if(dbpedia_items is None):
            print("0 results.")
            continue
        else:
            print(len(dbpedia_items), "results.")

    return dbpedia_items


# Main query routine

# Sample link http://dexterdemo.isti.cnr.it:8080/dexter-webapp/api/rest/spot-entities?text=Bob%20Dylan&wn=false&debug=false&format=text
def find_dbpedia_items(query):
    payload = {'wn': 'false',
               'debug': 'false',
               'text': query,
               'format': 'text'}
    r = requests.get(DEXTER_API_URI, params=payload)
    print 'status code', r.status_code
    if(r.status_code != 200):
        print("FAILURE: Request", r.url, "failed")
        return None
    result = r.json()
    if result.get('entities') is None:
        return None
    else:
        return result['entities']



# Command line parsing

if __name__ == '__main__':

    parser = argparse.ArgumentParser(
                    description="DBPedia query.")
    parser.add_argument('query', type=str, nargs='?',
                    help="Input query text to be processed")

    if len(sys.argv) < 1:
        parser.print_help()
        sys.exit(1)

    args = parser.parse_args()
    find_dbpedia_items(args.query)
