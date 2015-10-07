#!/usr/bin/env python
"""
Script for summarizing data for statistics generation.

Invocation:
$ python mediawiki_helper.py data/normalized/*.json -o summary_authors.csv
"""

import argparse
import csv
import sys
import codecs

from simplejson import JSONDecodeError

import json

import common
import glob

import freebase_helper

import os


ONB_COL = 0
NAME_COL = 1
GND_COL = 2
SLASH = '/'
UNDERSCORE = '_'
BLANK = ' '
MAX_PROP_ID = 2500


WIKIDATA_API_ACTION = 'https://www.wikidata.org/w/api.php?action='
WIKIDATA_API_URL = WIKIDATA_API_ACTION + 'wbgetentities&ids=Q'
TMP_WIKIDATA_API_URL = 'https://wdq.wmflabs.org/api?q='
ITEMS_JSON = 'items'
LANGUAGE_EN = '&languages=en'
FORMAT_JSON = '&format=json'
JSON_EXT = '.json'
VALUE_POS_IN_WIKIDATA_PROP_LIST = 2
WIKIDATA_AUTHOR_DIR = 'data/mediawikidata_author_dir'
WIKIDATA_PROPERTY_DIR = 'data/mediawikidata_property_dir'
WIKIDATA_AUTHOR_DATA_DIR = 'data/mediawikidata_author_data_dir'
CATEGORIES_FILE = 'data/categories.csv'
WIKIDATA_PROP_FILE = 'data/wikidata_prop.csv'


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

wikidata_property_fieldnames = [
    'id'
    , 'name'
]


def extract_property_value(response, property):

    values = ''
    try:
        json_data = response
        for value_list in json_data['P' + str(property)]:
            value = value_list['mainsnak']['datavalue']['value']
            if values=='':
                values = value
            else:
                values = values + ' ' + value

            #property_data_list = json_data[PROPS_JSON][str(property)]
            #values = " ".join(str(value_list[VALUE_POS_IN_WIKIDATA_PROP_LIST]) for value_list in property_data_list)
    except JSONDecodeError as jde:
        print 'JSONDecodeError. Response author data:', response, jde
#        print 'JSONDecodeError. Response author data:', response.content, jde
    except:
#        print 'Response json:', response.content
        print 'Response json:', response
        print 'Unexpected error:', sys.exc_info()[0]
    print 'property:', property, 'values:', values
    return common.toByteStr(values)


def extract_property_id(response, property):

    values = ''
    try:
        json_data = response
        for value_list in json_data['P' + str(property)]:
            value = value_list['mainsnak']['datavalue']['value']['numeric-id']
            if values=='':
                values = value
            else:
                values = values + ' ' + value

            #property_data_list = json_data[PROPS_JSON][str(property)]
            #values = " ".join(str(value_list[VALUE_POS_IN_WIKIDATA_PROP_LIST]) for value_list in property_data_list)
    except JSONDecodeError as jde:
        print 'JSONDecodeError. Response author data:', response, jde
#        print 'JSONDecodeError. Response author data:', response.content, jde
    except:
#        print 'Response json:', response.content
        print 'Response json:', response
        print 'Unexpected error:', sys.exc_info()[0]
    print 'property:', property, 'values:', values
    return str(values)


def extract_occupation_label(response, property):

    values = ''
    try:
        json_data = response
        values = json_data['entities']['Q'+str(property)]['labels']['en']['value']
    except JSONDecodeError as jde:
        print 'JSONDecodeError. Response author data:', response, jde
    except:
        print 'Response json:', response
        print 'Unexpected error:', sys.exc_info()[0]
    print 'property:', property, 'values:', values
    return common.toByteStr(values)


def extract_property_label(response, property):

    values = ''
    try:
        json_data = response
        values = json_data['entities']['P'+str(property)]['labels']['en']['value']
    except JSONDecodeError as jde:
        print 'JSONDecodeError. Response property data:', response, jde
    except:
        print 'Response json:', response
        print 'Unexpected error:', sys.exc_info()[0]
    print 'property:', property, 'values:', values
    return common.toByteStr(values)


def extract_wikidata_author_id(response):

    try:
#        return response.json()[ITEMS_JSON][0]
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
    genres = extract_property_id(author_response_json, GENRE_PROP)
    occupations = extract_property_id(author_response_json, OCCUPATION_PROP)
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

    category = extract_occupation_label(wikidata_occupation_data_response_json, occupation_id)

    values = [
        wikidata_author_id
        , occupation_id
        , category
    ]

    return dict(zip(wikidata_category_fieldnames, values))


def build_wikidata_property_entry(
        wikidata_property_response_json, id):

    name = extract_property_label(wikidata_property_response_json, id)

    values = [
        id
        , name
    ]

    return dict(zip(wikidata_property_fieldnames, values))


# query Wikidata by GND ID, where GND Identifier is a property P227
# e.g. https://wdq.wmflabs.org/api?q=string[227:118576291] for Gustav Mahler
def retrieve_wikidata_author_id(gnd):

    query = TMP_WIKIDATA_API_URL + 'string[' + str(GND_ID_PROP) + ':' + gnd + ']'
    print 'query:', query
    wikidata_author_id_response = common.process_http_query(query)
    print 'response content:', wikidata_author_id_response.content
    return wikidata_author_id_response


# query Wikidata by wikidata ID for an author
# e.g. https://www.wikidata.org/w/api.php?action=wbgetentities&ids=Q7304&languages=en&format=json
def retrieve_wikidata_author_data(wikidata_author_id):

    query_author = WIKIDATA_API_URL + str(wikidata_author_id) + LANGUAGE_EN + FORMAT_JSON
    print 'query author:', query_author
    author_response_json = common.process_http_query(query_author)
    print 'author json data:', author_response_json
    return author_response_json


# query Wikidata by wikidata ID for a conductor occupation, which is represented
# by property 'Commons Category'
# e.g. https://www.wikidata.org/wiki/Q158852 for conductor in browser
#      https://wdq.wmflabs.org/api?q=items[158852]&props=373 in API
def retrieve_wikidata_occupation(wikidata_occupation_id):

    query_occupation = WIKIDATA_API_URL + str(wikidata_occupation_id) + LANGUAGE_EN + FORMAT_JSON
    print 'query occupation:', query_occupation
    occupation_response_json = common.process_http_query(query_occupation)
    print 'occupation json data:', occupation_response_json
    return occupation_response_json


# query Wikidata by property ID for property descripton
# e.g. https://www.wikidata.org/w/api.php?action=wbgetentities&ids=P17
def retrieve_wikidata_property(property_id):

    query_property = WIKIDATA_API_ACTION + 'wbgetentities&ids=P' + str(property_id) + FORMAT_JSON
    print 'query property:', query_property
    response_json = common.process_http_query(query_property)
    print 'json data:', response_json
    return response_json


def add_occupation(occupation_id, wikidata_author_id):

    occupation_data_response = retrieve_wikidata_occupation(occupation_id)
    #wikidata_occupation_data_response_json = common.validate_response_json(occupation_data_response)
    wikidata_occupation_data_response_json = json.loads(occupation_data_response.content)
    entry = build_wikidata_occupation_entry(wikidata_occupation_data_response_json, occupation_id, wikidata_author_id)
    with open(CATEGORIES_FILE, 'ab') as csvfile:
        writer = csv.DictWriter(csvfile, delimiter=';', fieldnames=wikidata_category_fieldnames, lineterminator='\n')
        writer.writerow(entry)


def load_properties():

    with codecs.open(WIKIDATA_PROP_FILE, 'w') as csvfile:
        writer = csv.DictWriter(csvfile, delimiter=';', fieldnames=wikidata_property_fieldnames, lineterminator='\n')
        writer.writeheader()
        for id in range(MAX_PROP_ID)[1:]:
            wikidata_response_json = None
            store = True
            inputfile = glob.glob(WIKIDATA_PROPERTY_DIR + SLASH + str(id))
            if(inputfile):
                print 'exists:', inputfile, 'for property:', id
                wikidata_response_content = common.read_json_file(inputfile[0])
                wikidata_response_json = json.loads(wikidata_response_content)
            else:
                print 'file not exists for property:', id
                wikidata_response = retrieve_wikidata_property(id)
                try:
                    wikidata_response_json = json.loads(wikidata_response.content)
                    store_wikidata_property(id, wikidata_response_json)
                except Exception as ex:
                    store = False
                    print 'property response error. ID:', id, ex

            if store:
                entry = build_wikidata_property_entry(wikidata_response_json, id)
                with open(WIKIDATA_PROP_FILE, 'ab') as csvfile:
                    writer = csv.DictWriter(csvfile, delimiter=';', fieldnames=wikidata_property_fieldnames, lineterminator='\n')
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
    inputfile = glob.glob(WIKIDATA_AUTHOR_DIR + SLASH + row[ONB_COL] + UNDERSCORE + gnd + '*')
    if(inputfile):
        print 'exists:', inputfile, 'for ONB:', row[ONB_COL]
        wikidata_author_id_response_content = common.read_json_file(inputfile[0])
        wikidata_author_id_response_json = json.loads(wikidata_author_id_response_content)
    else:
        print 'onb_wikidata not exists for ONB:', row[ONB_COL]
        wikidata_author_id_response = retrieve_wikidata_author_id(gnd)
        wikidata_author_id_response_json = wikidata_author_id_response.json()
        wikidata_author_id_response_content = wikidata_author_id_response.content
    wikidata_author_id = extract_wikidata_author_id(wikidata_author_id_response_json)
    print 'wikidata_author_id', wikidata_author_id
    store_wikidata_author_id(line, wikidata_author_id, gnd, wikidata_author_id_response_content)
    return wikidata_author_id


# store Wikidata author ID response in format {wikidata-id}_{onb-id}.json
def store_wikidata_author_id(line, author_id, gnd, response):

    row = line.split(";")
    common.write_json_file(WIKIDATA_AUTHOR_DIR, str(row[ONB_COL]) + UNDERSCORE + gnd + UNDERSCORE + str(author_id), response)


# store Wikidata property response in format {property-id}.json
def store_wikidata_property(property_id, response):

    common.write_json_file(WIKIDATA_PROPERTY_DIR, str(property_id) + JSON_EXT, response)


# store Wikidata author data response in format {wikidata-id}.json
def store_wikidata_author_data(author_id, response):

    common.write_json_file(WIKIDATA_AUTHOR_DATA_DIR, str(author_id), response)


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
            inputfile = glob.glob(WIKIDATA_AUTHOR_DATA_DIR + SLASH + str(wikidata_author_id) + '*')
            if(inputfile):
                print 'exists:', inputfile, 'for wikidata author ID:', wikidata_author_id
                wikidata_author_data_response_content = common.read_json_file(inputfile[0])
                #wikidata_author_data_response_content = common.validate_json_str(wikidata_author_data_response_content_tmp)
                wikidata_author_data_response_json = json.loads(wikidata_author_data_response_content)
            else:
                print 'wikidata not exists for wikidata author ID:', wikidata_author_id
                author_data_response = retrieve_wikidata_author_data(wikidata_author_id)
                #wikidata_author_data_response_json = common.validate_response_json(author_data_response)
                wikidata_author_data_response_json = json.loads(author_data_response.content)
                wikidata_author_data_response_content = author_data_response.content
            store_wikidata_author_data(wikidata_author_id, wikidata_author_data_response_content)
            property_dict = wikidata_author_data_response_json['entities']['Q'+str(wikidata_author_id)]['claims']
            entry = build_wikidata_author_entry(property_dict, line, wikidata_author_id)
            writer.writerow(entry)


# Main mapping routine

def map_records(inputfile, outputfile):

    print("Mapping", len(inputfile), "records in", outputfile)
    os.remove(CATEGORIES_FILE)
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
                    description="Mapping identifiers and data for dataset authors. MediaWiki query.")
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
