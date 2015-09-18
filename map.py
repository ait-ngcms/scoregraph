#!/usr/bin/env python
"""
Script for summarizing data for statistics generation.

Invocation:
$ python map.py data/normalized/*.json -o summary_authors.csv
"""

import argparse
import csv
import sys
import codecs

import requests


ONB_COL = 0
NAME_COL = 1
GND_COL = 2


WIKIDATA_API_URL = 'https://wdq.wmflabs.org/api?q='
ITEMS_JSON = 'items'
PROPS_JSON = 'props'
VALUE_POS_IN_WIKIDATA_PROP_LIST = 2


OCCUPATION_PROP              = 106
VIAF_ID_PROP                 = 214
MUSIC_BRAINZ_ARTIST_ID_PROP  = 434
GND_ID_PROP                  = 227
BNF_ID_PROP                  = 268
NKC_ID_PROP                  = 691
FREEBASE_ID_PROP             = 646
GENRE_PROP                   = 136
IMSLP_ID_PROP                = 839
NTA_ID_PROP                  = 1006


properties = [
    OCCUPATION_PROP
    , VIAF_ID_PROP
    , MUSIC_BRAINZ_ARTIST_ID_PROP
    , GND_ID_PROP
    , BNF_ID_PROP
    , NKC_ID_PROP
    , FREEBASE_ID_PROP
    , GENRE_PROP
    , IMSLP_ID_PROP
    , NTA_ID_PROP
]

wikidata_author_fieldnames = [
    'gnd'
    , 'wikidata'
    , 'onb'
    , 'name'
    , 'genre'
    , 'occupation'
    , 'freebase'
    , 'viaf'
    , 'bnf'
    , 'nkc'
    , 'nta'
    , 'imslp'
    , 'dbpedia'
    , 'music_brainz_artist_id'
]


def process_http_query(query):

    r = requests.get(query)
    if(r.status_code != 200):
        print("Request error:", r.url)
    return r


def extract_property_value(response, property):

    values = ''
    try:
        json_data = response.json()
        if str(property) in json_data[PROPS_JSON]:
            property_data_list = json_data[PROPS_JSON][str(property)]
            values = " ".join(str(value_list[VALUE_POS_IN_WIKIDATA_PROP_LIST]) for value_list in property_data_list)
    except:
        print "Unexpected error:", sys.exc_info()[0]
    print 'property:', property, 'values:', values
    return values


def extract_wikidata_author_id(response):

    try:
        return response.json()[ITEMS_JSON][0]
    except IndexError as ie:
        print 'No items found!', ie
    return None


def build_wikidata_author_entry(
        author_response_json, line, wikidata_author_id):

    row = line.split(";")
    genres = extract_property_value(author_response_json, GENRE_PROP)
    occupations = extract_property_value(author_response_json, OCCUPATION_PROP)
    freebase = extract_property_value(author_response_json, FREEBASE_ID_PROP)
    viaf = extract_property_value(author_response_json, VIAF_ID_PROP)
    bnf = extract_property_value(author_response_json, BNF_ID_PROP)
    nkc = extract_property_value(author_response_json, NKC_ID_PROP)
    nta = extract_property_value(author_response_json, NTA_ID_PROP)
    imslp = extract_property_value(author_response_json, IMSLP_ID_PROP)
    music_brainz_artist = extract_property_value(author_response_json, MUSIC_BRAINZ_ARTIST_ID_PROP)

    values = [
        row[GND_COL]
        , wikidata_author_id
        , row[ONB_COL]
        , row[NAME_COL]
        , genres
        , occupations
        , freebase
        , viaf
        , bnf
        , nkc
        , nta
        , imslp
        , ''
        , music_brainz_artist
    ]

    return dict(zip(wikidata_author_fieldnames, values))


# query Wikidata by GND ID, where GND Identifier is a property P227
# e.g. https://wdq.wmflabs.org/api?q=string[227:118576291] for Gustav Mahler
def retrieve_wikidata_author_id(gnd):

    query = WIKIDATA_API_URL + 'string[' + str(GND_ID_PROP) + ':' + gnd + ']'
    print 'query:', query
    wikidata_author_id_response = process_http_query(query)
    print 'response content:', wikidata_author_id_response.content
    return wikidata_author_id_response


# query Wikidata by wikidata ID for an author
# e.g. https://www.wikidata.org/wiki/Q7304 for Gustav Mahler in browser
#      https://wdq.wmflabs.org/api?q=items[7304]&props=106,136 in API
def retrieve_wikidata_author_data(wikidata_author_id):

    query_author = WIKIDATA_API_URL + ITEMS_JSON + '[' + str(wikidata_author_id) + ']&' + \
                   PROPS_JSON + '=' + ", ".join(str(e) for e in properties)
    print 'query author:', query_author
    author_response_json = process_http_query(query_author)
    print 'author json data:', author_response_json
    return author_response_json


def extract_gnd_from_line(line):

    try:
        row = line.split(";")
        return row[GND_COL].split('/')[-1]
    except IndexError as ie:
        print 'No GND found!', ie
    return None


def get_wikidata_author_id_by_gnd(gnd):

    wikidata_author_id_response = retrieve_wikidata_author_id(gnd)
    wikidata_author_id = extract_wikidata_author_id(wikidata_author_id_response)
    print 'wikidata_author_id', wikidata_author_id
    return wikidata_author_id


def store_author_data_by_gnd(inputfile, writer):

    # GND cache contains aggregated GND IDs
    gnd_cache = []

    f = codecs.open(inputfile, 'r')
    for idx, line in enumerate(f):
        print repr(line)
        if idx > 0:
            gnd = extract_gnd_from_line(line)
            store_author_data(writer, gnd, gnd_cache, line)


def store_author_data(writer, gnd, gnd_cache, line):

    if gnd not in gnd_cache:
        wikidata_author_id = get_wikidata_author_id_by_gnd(gnd)
        if(wikidata_author_id and wikidata_author_id not in gnd_cache):
            gnd_cache.append(wikidata_author_id)
            author_response_json = retrieve_wikidata_author_data(wikidata_author_id)
            # store author wikidata JSON in file onb_id_wikidata_id.json
            entry = build_wikidata_author_entry(author_response_json, line, wikidata_author_id)
            writer.writerow(entry)


# Main mapping routine

def map_records(inputfile, outputfile):

    print("Mapping", len(inputfile), "records in", outputfile)
    with open(outputfile, 'w') as csvfile:
        writer = csv.DictWriter(csvfile, delimiter=';', fieldnames=wikidata_author_fieldnames, lineterminator='\n')
        writer.writeheader()
        store_author_data_by_gnd(inputfile, writer)


# Command line parsing

if __name__ == '__main__':

    parser = argparse.ArgumentParser(
                    description="Mapping identifiers and data for dataset authors.")
    parser.add_argument('inputfile', type=str, nargs='+',
                    help="Input file to be processed")
    parser.add_argument('-o', '--outputfile', type=str, nargs='?',
                    default="data/mapping.csv",
                    help="Output file")


    if len(sys.argv) < 2:
        parser.print_help()
        sys.exit(1)

    args = parser.parse_args()
    map_records(args.inputfile, args.outputfile)
