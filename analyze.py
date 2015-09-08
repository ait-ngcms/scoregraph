#!/usr/bin/env python
"""
Script for analyzing data for statistics generation.

Data to analyze is comprised in the folder 'data' that should have subdirectories:
    'raw'        for initial XML files                          (input data)
    'normalized' that contains normalized data in JSON format   (output data)
    'enhanced'   that contains data enhanced by analyzing       (output data)

Statistics are generated in:
    summary_normalized.csv
    summary_enriched.csv

Invocation:
$ python analyze.py data
"""

import argparse
import os
import sys

# directory structure
from os import walk

# parts of analysis
import normalize
import enrich
import summarize

# HTTP connection
import requests

import urllib

from SPARQLWrapper import SPARQLWrapper, JSON

DEXTER_API_URI            = "http://dexterdemo.isti.cnr.it:8080/dexter-webapp/api/rest/spot-entities"
DEXTER_API_DBPEDIA_ID_URI = "http://dexterdemo.isti.cnr.it:8080/dexter-webapp/api/rest/get-desc?"

def analyze(inputdir, dirnames):

    mode_raw = 'raw'
    mode_normalized = 'normalized'
    mode_enriched = 'enriched'
    raw_path = inputdir + '/' + mode_raw
    normalized_path = inputdir + '/' + mode_normalized
    enriched_path = inputdir + '/' + mode_enriched

    # normalize enitities
    if mode_raw in dirnames:
        raw_files = os.listdir(raw_path)
        raw_files = [(raw_path + '/' + element) for element in raw_files]
        normalize.normalize_records(raw_files, normalized_path)
    else:
        print 'Error. ' +  mode_raw + ' folder is missing.'

    # enrich entities with Europeana data using GND number
    if mode_normalized in dirnames:
        normalized_files = os.listdir(normalized_path)
        normalized_files = [(normalized_path + '/' + element) for element in normalized_files]
        enrich.enrich_records(normalized_files, enriched_path)

        # summarize statistics
        if mode_enriched in dirnames:
            summarize.summarize_records(normalized_files, inputdir + '/summary_' + mode_normalized + '.csv')
            enriched_files = os.listdir(enriched_path)
            enriched_files = [(enriched_path + '/' + element) for element in enriched_files]
            summarize.summarize_records(enriched_files, inputdir + '/summary_' + mode_enriched + '.csv')
            summarize.summarize_titles(normalized_files, inputdir + '/summary_titles.csv')
            summarize.summarize_authors(normalized_files, inputdir + '/summary_authors.csv')
        else:
            print 'Error. ' + mode_enriched + ' folder is missing.'
    else:
        print 'Error. ' + mode_normalized + ' folder is missing.'

    # spot entities - detects all the mentions in DBPedia that could refer to an entity in the text
    query_list = summarize.read_summary(inputdir + '/summary_titles.csv')
    print("\tSearching Dexter for author, subject and title.")

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

    print '+++ Analyzing completed +++'


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


# sample link http://dexterdemo.isti.cnr.it:8080/dexter-webapp/api/rest/get-desc?id=49109&title-only=false
def find_dbpedia_id(query):
    payload = {'id': query,
               'title-only': 'false'}
    r = requests.get(DEXTER_API_DBPEDIA_ID_URI, params=payload)
    print 'find DBPedia ID status code', r.status_code
    if(r.status_code != 200):
        print("FAILURE: Request", r.url, "failed")
        return None
    result = r.json()
    return result.get('url')


# Main analyzing routine

def analyze_records(inputdir):
    print("Analyzing '" + inputdir + "' records.")

    for (dirpath, dirnames, filenames) in walk(inputdir):
        analyze(inputdir, dirnames)
        break


# Command line parsing

if __name__ == '__main__':

    parser = argparse.ArgumentParser(
                    description="Analyzing data for dataset statistics.")
    parser.add_argument('inputdir', type=str, #nargs='+',
                    help="Input files to be processed")

    if len(sys.argv) < 1:
        parser.print_help()
        sys.exit(1)

    args = parser.parse_args()
    analyze_records(args.inputdir)
