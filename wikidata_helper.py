#!/usr/bin/env python
"""
Script for summarizing data for statistics generation.

Invocation:
$ python wikidata_helper.py data/normalized/*.json -o summary_authors.csv
"""

import argparse
import csv
import sys
import codecs

from simplejson import JSONDecodeError

import json

import common

import freebase_helper

import summarize


ONB_COL = 0
NAME_COL = 1
GND_COL = 2
SLASH = '/'
UNDERSCORE = '_'
BLANK = ' '
JSON_EXT = '.json'

WIKIDATA_API_URL = 'https://wdq.wmflabs.org/api?q='
ITEMS_JSON = 'items'
PROPS_JSON = 'props'
VALUE_POS_IN_WIKIDATA_PROP_LIST = 2
WIKIDATA_AUTHOR_DIR = 'data/wikidata_author_dir'
WIKIDATA_AUTHOR_DATA_DIR = 'data/wikidata_author_data_dir'
WIKIDATA_COMPOSITION_DATA_DIR = 'data/wikidata_composition_data_dir'
CATEGORIES_FILE = 'data/categories.csv'


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

COMMONS_CATEGORY_PROP        = 373


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

wikidata_category_fieldnames = [
    'wikidata'
    , 'occupation_id'
    , 'category'
]


def extract_property_value(response, property):

    values = ''
    try:
        json_data = response
        if str(property) in json_data[PROPS_JSON]:
            property_data_list = json_data[PROPS_JSON][str(property)]
            values = " ".join(str(value_list[VALUE_POS_IN_WIKIDATA_PROP_LIST]) for value_list in property_data_list)
    except JSONDecodeError as jde:
        print 'JSONDecodeError. Response author data:', response, jde
    except:
        print 'Response json:', response
        print 'Unexpected error:', sys.exc_info()[0]
    print 'property:', property, 'values:', values
    return values


def extract_wikidata_author_id(response):

    try:
        return response[ITEMS_JSON][0]
    except JSONDecodeError as jde:
        print 'JSONDecodeError. Response author ID:', response.content
        print 'Incorrect JSON syntax!', jde
    except IndexError as ie:
        print 'No items found!', ie
    return None


def build_wikidata_author_entry(
        author_response_json, line, wikidata_author_id):

    row = line.split(";")
    genres = extract_property_value(author_response_json, GENRE_PROP)
    occupations = extract_property_value(author_response_json, OCCUPATION_PROP)
    for occupation in occupations.split(BLANK):
        add_occupation(occupation, wikidata_author_id)
    freebase = extract_property_value(author_response_json, FREEBASE_ID_PROP)
    for id in freebase.split(BLANK):
        freebase_helper.retrieve_compositions(id)
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


def build_wikidata_occupation_entry(
        wikidata_occupation_data_response_json, occupation_id, wikidata_author_id):

    category = extract_property_value(wikidata_occupation_data_response_json, COMMONS_CATEGORY_PROP)

    values = [
        wikidata_author_id
        , occupation_id
        , category
    ]

    return dict(zip(wikidata_category_fieldnames, values))


# query Wikidata by GND ID, where GND Identifier is a property P227
# e.g. https://wdq.wmflabs.org/api?q=string[227:118576291] for 'Gustav Mahler'
def retrieve_wikidata_author_id(gnd):

    query = WIKIDATA_API_URL + 'string[' + str(GND_ID_PROP) + ':' + gnd + ']'
    print 'query:', query
    wikidata_author_id_response = common.process_http_query(query)
    print 'response content:', wikidata_author_id_response.content
    return wikidata_author_id_response


# query Wikidata by Freebase ID, where Freebase Identifier is a property P646
# e.g. https://wdq.wmflabs.org/api?q=string[646:/m/02pbjrj] for 'Die Dollarprinzessin'
def retrieve_wikidata_composition_by_freebase_id(freebase_id):

    query = WIKIDATA_API_URL + 'string[' + str(FREEBASE_ID_PROP) + ':' + freebase_id + ']'
    print 'query wikidata composition:', query
    wikidata_composition_response = common.process_http_query(query)
    print 'response wikidata composition content:', wikidata_composition_response.content
    return wikidata_composition_response


def retrieve_wikidata_compositions_by_freebase_id(inputfile):

    summary = summarize.read_csv_summary(inputfile)
    for row in summary[1:]: # ignore first row, which is a header
        FREEBASE_ID_COL = 1
        print row[FREEBASE_ID_COL]
        wikidata_composition_response = retrieve_wikidata_composition_by_freebase_id(row[FREEBASE_ID_COL])
        print wikidata_composition_response
        try:
            wikidata_composition_response_json = json.loads(wikidata_composition_response.content)
            items = wikidata_composition_response_json[ITEMS_JSON]
            if len(items) > 0:
                wikidata_composition_id = items[0]
                print 'wikidata_composition_id:', wikidata_composition_id
                composition_response_json = common.is_stored_as_json_file(
                    WIKIDATA_API_URL + ITEMS_JSON + '[' + str(wikidata_composition_id) + ']&' + PROPS_JSON + '=*')
                if(composition_response_json == None):
                #inputfile = glob.glob(WIKIDATA_COMPOSITION_DATA_DIR + SLASH + str(wikidata_composition_id))
                #if not inputfile:
                    print 'composition data not exists for composition:', wikidata_composition_id
                    #composition_response_json = retrieve_wikidata_composition_data(wikidata_composition_id)
                    print 'composition json:', composition_response_json
                    store_wikidata_composition_data(wikidata_composition_id, composition_response_json)
#                    store_wikidata_composition_data(wikidata_composition_id, composition_response_json.content)
        except KeyError as ke:
            print 'no composition items found:', row[FREEBASE_ID_COL], ke


#def retrieve_wikidata_composition_data(wikidata_composition_id):

#    query_composition = WIKIDATA_API_URL + ITEMS_JSON + '[' + str(wikidata_composition_id) + ']&' + \
#                   PROPS_JSON + '=*'
#    print 'query composition:', query_composition
#    composition_response_json = common.process_http_query(query_composition)
#    print 'composition json data:', composition_response_json
#    return composition_response_json


# query Wikidata by wikidata ID for an author
# e.g. https://www.wikidata.org/wiki/Q7304 for Gustav Mahler in browser
#      https://wdq.wmflabs.org/api?q=items[7304]&props=106,136 in API
def retrieve_wikidata_author_data(wikidata_author_id):

    query_author = WIKIDATA_API_URL + ITEMS_JSON + '[' + str(wikidata_author_id) + ']&' + \
                   PROPS_JSON + '=' + ", ".join(str(e) for e in properties)
    print 'query author:', query_author
    author_response_json = common.process_http_query(query_author)
    print 'author json data:', author_response_json
    return author_response_json


# query Wikidata by wikidata ID for a conductor occupation, which is represented
# by property 'Commons Category'
# e.g. https://www.wikidata.org/wiki/Q158852 for conductor in browser
#      https://wdq.wmflabs.org/api?q=items[158852]&props=373 in API
def retrieve_wikidata_occupation(wikidata_occupation_id):

    query_occupation = WIKIDATA_API_URL + ITEMS_JSON + '[' + str(wikidata_occupation_id) + ']&' + \
                   PROPS_JSON + '=' + str(COMMONS_CATEGORY_PROP)
    print 'query occupation:', query_occupation
    occupation_response_json = common.process_http_query(query_occupation)
    print 'occupation json data:', occupation_response_json
    return occupation_response_json


def add_occupation(occupation_id, wikidata_author_id):

    occupation_data_response = retrieve_wikidata_occupation(occupation_id)
    wikidata_occupation_data_response_json = common.validate_response_json(occupation_data_response)
    entry = build_wikidata_occupation_entry(wikidata_occupation_data_response_json, occupation_id, wikidata_author_id)
    with open(CATEGORIES_FILE, 'ab') as csvfile:
        writer = csv.DictWriter(csvfile, delimiter=';', fieldnames=wikidata_category_fieldnames, lineterminator='\n')
        writer.writerow(entry)



def extract_gnd_from_line(line):

    try:
        row = line.split(";")
        return row[GND_COL].split(SLASH)[-1]
    except IndexError as ie:
        print 'No GND found!', ie
    return None


def get_wikidata_author_id_by_gnd(gnd, line):

    row = line.split(";")
    wikidata_author_id_response_json = common.is_stored_as_json_file(
        WIKIDATA_AUTHOR_DIR + SLASH + row[ONB_COL] + UNDERSCORE + gnd + '*')
    if(wikidata_author_id_response_json == None):
        print 'onb_wikidata not exists for ONB:', row[ONB_COL]
        wikidata_author_id_response = retrieve_wikidata_author_id(gnd)
        wikidata_author_id_response_json = wikidata_author_id_response.json()
    wikidata_author_id = extract_wikidata_author_id(wikidata_author_id_response_json)
    print 'wikidata_author_id', wikidata_author_id
    store_wikidata_author_id(line, wikidata_author_id, gnd, wikidata_author_id_response_json)
    return wikidata_author_id


# store Wikidata author ID response in format {wikidata-id}_{onb-id}.json
def store_wikidata_author_id(line, author_id, gnd, response):

    row = line.split(";")
    common.write_json_file(WIKIDATA_AUTHOR_DIR, str(row[ONB_COL]) + UNDERSCORE + gnd + UNDERSCORE + str(author_id) + JSON_EXT, response)


# store Wikidata author data response in format {wikidata-id}.json
def store_wikidata_author_data(author_id, response):

    common.write_json_file(WIKIDATA_AUTHOR_DATA_DIR, str(author_id) + JSON_EXT, response)


# store Wikidata composition data response in format {wikidata-id}.json
def store_wikidata_composition_data(composition_id, response):

    common.write_json_file(WIKIDATA_COMPOSITION_DATA_DIR, str(composition_id) + JSON_EXT, response)


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
        wikidata_author_id = get_wikidata_author_id_by_gnd(gnd, line)
        if(wikidata_author_id and wikidata_author_id not in gnd_cache):
            gnd_cache.append(wikidata_author_id)
            wikidata_author_data_response_json = common.is_stored_as_json_file(
                WIKIDATA_AUTHOR_DATA_DIR + SLASH + str(wikidata_author_id) + '*')
            if(wikidata_author_data_response_json == None):
                print 'wikidata not exists for wikidata author ID:', wikidata_author_id
                author_data_response = retrieve_wikidata_author_data(wikidata_author_id)
                wikidata_author_data_response_json = common.validate_response_json(author_data_response) #author_data_response.json()
            store_wikidata_author_data(wikidata_author_id, wikidata_author_data_response_json)
            entry = build_wikidata_author_entry(wikidata_author_data_response_json, line, wikidata_author_id)
            writer.writerow(entry)


# Main mapping routine

def map_records(inputfile, outputfile):

    print("Mapping", len(inputfile), "records in", outputfile)
    with codecs.open(CATEGORIES_FILE, 'w') as csvfile:
        writer = csv.DictWriter(csvfile, delimiter=';', fieldnames=wikidata_category_fieldnames, lineterminator='\n')
        writer.writeheader()
    with open(outputfile, 'w') as csvfile:
        writer = csv.DictWriter(csvfile, delimiter=';', fieldnames=wikidata_author_fieldnames, lineterminator='\n')
        writer.writeheader()
        store_author_data_by_gnd(inputfile, writer)


# Command line parsing

if __name__ == '__main__':

    parser = argparse.ArgumentParser(
                    description="Mapping identifiers and data for dataset authors. Wikidata query.")
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
