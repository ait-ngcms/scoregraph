#!/usr/bin/env python
"""
Script for enriching normalized data with contextually relevant data.

Invocation:
$ python enrich.py data/normalized/*.json -o data/enriched -a EUROPEANA_API_KEY
"""

import argparse
import json
import os
import sys

import requests
from bs4 import BeautifulSoup

from common import write_json_file, progress, read_records

# Europeana enrichment

EUROPEANA_API_URI  = "http://europeana.eu/api/v2/search.json?"
EUROPEANA_MAX_ROWS = 20
EUROPEANA_API_KEY  = open("europeana_api_key").read()



def extract_europeana_data(europeana_items):
    data = []
    for item in europeana_items:
        data.append({'id': item.get('id'),
                     'creator': item.get('dcCreator'),
                     'title': item.get('title'),
                     'score': item.get('score')})
    return data


def find_europeana_items(query):
    payload = {'wskey': EUROPEANA_API_KEY,
               'profile': 'standard',
               'query': query,
               'start': 1,
               'rows': EUROPEANA_MAX_ROWS}
    r = requests.get(EUROPEANA_API_URI, params=payload)
    if(r.status_code != 200):
        print("FAILURE: Request", r.url, "failed")
        return None
    result = r.json()
    if result.get('items') is None:
        return None
    else:
        return result['items']


def find_europeana_items_ext(query):
    payload = {'wskey': EUROPEANA_API_KEY,
               'profile': 'standard',
               'query': query,
               'start': 1,
               'rows': 200}
    r = requests.get(EUROPEANA_API_URI, params=payload)
    print 'Europeana query URL:', r.url
    if(r.status_code != 200):
        print("FAILURE: Request", r.url, "failed")
        return None
    result = r.json()
    if result.get('totalResults') is None:
        return None
    else:
        return result['totalResults']


def extract_europeana_data(europeana_items):
    data = []
    for item in europeana_items:
        data.append({'id': item.get('id'),
                     'creator': item.get('dcCreator'),
                     'title': item.get('title'),
                     'score': item.get('score')})
    return data


def filter_europeana_items(data, europeana_items):
    # collect all known person URIs
    known_person_uris = []
    if 'persons' in data:
        for person in data['persons']:
            if 'sameas' in person:
                for uri in person['sameas']:
                    known_person_uris.append(uri)
    # keep only those items that have at least one matching person URI
    filtered_items = []
    for item in europeana_items:
        if item.get('dcCreator'):
            for term in item['dcCreator']:
                if term in known_person_uris:
                    filtered_items.append(item)
    return filtered_items


def enrich_europeana(data):
    """Enriches a normalized record with Europeana data"""
    # TODO: improve and remove duplicates
    title = data.get('title')
    if 'persons' not in data:
        return data
    for person in data['persons']:
        name = person['name']
        query = name
        if title:
            query = query + " " + title

        # all items returned by Europeana
        #print("\tSearching Europeana for '", query, end="' ...")
        print("\tSearching Europeana for '", query)
        print("' ...")

        europeana_items = find_europeana_items(query)
        if(europeana_items is None):
            print("0 results.")
            continue
        else:
            print(len(europeana_items), "results.")
        # filter items
        europeana_items = filter_europeana_items(data, europeana_items)
        # extract enrichments
        enrichments = extract_europeana_data(europeana_items)
        if(len(enrichments) == 0):
            print("\tNo enrichments found.")
            continue
        print("\tEnriching record with", len(enrichments), "related objects.")
        if data.get('related_europeana_items') is not None:
            data['related_europeana_items'].extend(enrichments)
        else:
            data['related_europeana_items'] = enrichments
    return data


def count_europeana_items(name):
    """Counts compositions in Europeana dataset"""
    query = name

    # all items returned by Europeana
    print("\tSearching Europeana for '", query)
    print("' ...")

    europeana_items = find_europeana_items_ext(query)
#    if(europeana_items is None):
#        print("0 results.")
#        return 0
#    else:
#        print(len(europeana_items), "results.")
#        return len(europeana_items)
    print(europeana_items, "results.")
    return europeana_items


# GND enrichment

GND_URI_PATTERN = "{GND_URI}/about/rdf"

def collect_sameas_uris(gnd_uri):
    same_as_uris = []
    url = GND_URI_PATTERN.replace("{GND_URI}", gnd_uri)
    headers = {'Accept': 'application/rdf+xml'}
    r = requests.get(url, allow_redirects=True)
    if(r.status_code != 200):
        print("Request error:", r.url)
        return same_as_uris
    #print(r.text)
    soup = BeautifulSoup(r.text)
    owl_sameas_tags = soup.find_all('owl:sameas')
    for tag in owl_sameas_tags:
        same_as_uris.append(tag['rdf:resource'])
    return same_as_uris


def enrich_gnd(data):
    # traverse all elements, find GND links, pull sameas uris
    for key in data.keys():
        value = data.get(key)
        if(isinstance(value, dict)):
            data[key] = enrich_gnd(value)
        if(isinstance(value, list)):
            if(key == 'sameas'):
                same_as_uris = []
                for uri in value:
                    if uri is None:
                        continue
#                    print("\tResolving GND uri", uri, end="...")
                    print("\tResolving GND uri", uri)
                    print("...")
                    collected_uris = collect_sameas_uris(uri)
                    print("found", len(collected_uris), "links.")
                    same_as_uris.extend(collected_uris)
                data[key].extend(same_as_uris)
            else:
                for val in value:
                    if(isinstance(val, dict)):
                        val = enrich_gnd(val)
    return data


# Main enrichment routine

def enrich(normalized_record):
    normalized_record = json.loads(normalized_record)
    print("\nProcessing record", normalized_record['aleph_id'], "...")
    # enrich by fetching additional data from GND
    print("Start GND enrichment", "...")
    enriched_record = enrich_gnd(normalized_record)
    # enrich by finding related resources in Europeana
    print("Start Europeana enrichment", "...")
    enriched_record = enrich_europeana(enriched_record)
    return enriched_record


def enrich_records(inputfiles, outputdir, force=False):
    print("Enriching", len(inputfiles), "records. Saving to", outputdir)
    for index, (filename, record) in enumerate(read_records(inputfiles)):
        progress((index+1)/len(inputfiles))
        out_file = os.path.basename(filename).replace(".json",
                                                      "_enriched.json")
        out_path = outputdir + "/" + out_file
        if(os.path.exists(out_path) and not force):
            print(out_file, "already enriched. Skipping...")
        else:
#            print("record: ", record)
            enriched_record = enrich(record)
            print("enriched_record: ", enriched_record)
            write_json_file(outputdir, out_file, enriched_record)


# Command line parsing

if __name__ == '__main__':

    parser = argparse.ArgumentParser(
                    description="Enriching normalized data with contextually relevant data.")
    parser.add_argument('inputfiles', type=str, nargs='+',
                    help="Input files to be processed")
    parser.add_argument('-o', '--outputdir', type=str, nargs='?',
                    default="data/enriched",
                    help="Output directory")
    parser.add_argument('-e', '--europeana_api_key', type=str, nargs=1,
                    help="Europeana API key")
    parser.add_argument('-f', '--force', type=bool, default=False,
                    help="Overwrite already existing enrichment files")


    if len(sys.argv) < 2:
        parser.print_help()
        sys.exit(1)

    args = parser.parse_args()
    EUROPEANA_API_KEY = args.europeana_api_key
    enrich_records(args.inputfiles, args.outputdir, args.force)
