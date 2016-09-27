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


WIKIDATA_API_URL = 'https://wdq.wmflabs.org/api?q='
ITEMS_JSON = 'items'
PROPS_JSON = 'props'

# Europeana facet collection
FACETS_JSON = 'facets'
FIELDS_JSON = 'fields'
LABEL_JSON = 'label'
COUNT_JSON = 'label'

VALUE_POS_IN_WIKIDATA_PROP_LIST = 2
WIKIDATA_AUTHOR_DIR = 'data/wikidata_author_dir'
WIKIDATA_COMPOSITION_DATA_DIR = 'data/wikidata_composition_data_dir'
WIKIDATA_BAND_DATA_DIR = 'data/wikidata_band_data_dir'
CATEGORIES_FILE = 'data/categories.csv'
FACET_COLLECTION_FILE = 'data/europeana_facet_collection.csv'
EUROPEANA_COLLECTION_URL = 'http://www.europeana.eu/api/v2/search.json?query=*%3A*&rows=0&facet=europeana_collectionName&profile=facets&f.europeana_collectionName.facet.limit=2000'


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
MUSICBRAINZ_COMPOSITION_ID_PROP = 435
COMMONS_CATEGORY_PROP        = 373
INTERNET_ARCHIVE_ID_PROP     = 724


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

wikidata_category_fieldnames = [
    'wikidata'
    , 'occupation_id'
    , 'category'
]


facet_collection_fieldnames = [
    'id'
    , 'label'
    , 'count'
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
#    if 'Stoll' in line:
#        ii = 0
    for occupation in occupations.split(common.BLANK):
        add_occupation(occupation, wikidata_author_id)
    freebase = extract_property_value(author_response_json, FREEBASE_ID_PROP)
    for id in freebase.split(common.BLANK):
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

    return dict(zip(common.wikidata_author_fieldnames, values))


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


# query Wikidata by Musicbrainz ID, where Musicbrainz Identifier is a property P435
# e.g. https://wdq.wmflabs.org/api?q=string[435:ad8df966-dba7-43d0-8d0e-91e69df5b5ef] for 'symphonie nr 2'
def retrieve_wikidata_composition_by_musicbrainz_id(musicbrainz_id):

    query = WIKIDATA_API_URL + 'string[' + str(MUSICBRAINZ_COMPOSITION_ID_PROP) + ':' + musicbrainz_id + ']'
    print 'query wikidata composition:', query
    wikidata_composition_response = common.process_http_query(query)
    print 'response wikidata composition content:', wikidata_composition_response.content
    return wikidata_composition_response


# query Wikidata by Internet Archive ID, where Internet Archive Identifier is a property P724
# e.g. https://wdq.wmflabs.org/api?q=string[724:PhilLeshandFriends]
def retrieve_wikidata_object_by_internet_archive_id(internet_archive_id):

    query = WIKIDATA_API_URL + 'string[' + str(INTERNET_ARCHIVE_ID_PROP) + ':' + internet_archive_id + ']'
    print 'query wikidata object:', query
    wikidata_object_response = common.process_http_query(query)
    print 'response wikidata object content:', wikidata_object_response.content
    return wikidata_object_response


# query Wikidata by wikidata composition ID for a VIAF id, which is represented
# by property 'VIAF ID'
# e.g. https://www.wikidata.org/wiki/Q5064 for 'The Magic Flute' in browser
#      https://wdq.wmflabs.org/api?q=items[5064]&props=214 in API
def retrieve_wikidata_composition_viaf_id_by_wikidata_id(wikidata_composition_id):

    composition_response_json = None
    try:
        query_composition = WIKIDATA_API_URL + ITEMS_JSON + '[' + str(wikidata_composition_id) + ']&' + \
                       PROPS_JSON + '=' + str(VIAF_ID_PROP)
        print 'query composition:', query_composition
        composition_response_json = common.process_http_query(query_composition)
    except:
        print 'No VIAF id found for composition ID:', wikidata_composition_id
    print 'composition json data:', composition_response_json
    return composition_response_json


def retrieve_wikidata_compositions_by_musicbrainz_id(inputfile, outputfile):

    with codecs.open(outputfile, 'w') as csvfile:
        writer = csv.DictWriter(csvfile, delimiter=';', fieldnames=common.map_compositions_fieldnames, lineterminator='\n')
        writer.writeheader()

        summary = summarize.read_csv_summary(inputfile)
        for row in summary[1:]: # ignore first row, which is a header
            MUSICBRAINZ_ID_COL = 0
            MUSICBRAINZ_AUTHOR_NAME_COL = 1
            MUSICBRAINZ_TITLE_COL = 2
            musicbrainz_id = row[MUSICBRAINZ_ID_COL]
            print musicbrainz_id
            wikidata_composition_response = retrieve_wikidata_composition_by_musicbrainz_id(musicbrainz_id)
            print wikidata_composition_response
            try:
                wikidata_composition_response_json = json.loads(wikidata_composition_response.content)
                items = wikidata_composition_response_json[ITEMS_JSON]
                viaf_id = 0
                wikidata_composition_id = 0
                if len(items) > 0:
                    wikidata_composition_id = items[0]
                    print 'wikidata_composition_id:', wikidata_composition_id
                    composition_response_json = common.is_stored_as_json_file(
                        WIKIDATA_API_URL + ITEMS_JSON + '[' + str(wikidata_composition_id) + ']&' + PROPS_JSON + '=*')
                    if(composition_response_json == None):
                        print 'composition data not exists for composition:', wikidata_composition_id
                        print 'composition json:', composition_response_json
                        store_wikidata_composition_data(wikidata_composition_id, composition_response_json)
                    wikidata_composition_viaf_response = retrieve_wikidata_composition_viaf_id_by_wikidata_id(wikidata_composition_id)
                    try:
                        #wikidata_composition_viaf_response_json = wikidata_composition_viaf_response.json()
                        wikidata_composition_viaf_response_json = json.loads(wikidata_composition_viaf_response.content)
                        #items = wikidata_composition_response_json[ITEMS_JSON]
                        viaf_id = extract_viaf_id_from_wikidata_composition_id(wikidata_composition_viaf_response_json)
                    except:
                        print 'No VIAF id found for composition ID:', wikidata_composition_id

                    print 'viaf id:', viaf_id

                entry = build_composition_mapping_entry(
                            row[MUSICBRAINZ_TITLE_COL]
                            , row[MUSICBRAINZ_AUTHOR_NAME_COL]
                            , wikidata_composition_id
                            , viaf_id
                            , musicbrainz_id
                )
                writer.writerow(entry)
            except KeyError as ke:
                print 'no composition items found:', row[MUSICBRAINZ_ID_COL], ke


def retrieve_wikidata_objects_by_internet_archive_id(inputfile, outputfile):

    with codecs.open(outputfile, 'w') as csvfile:
        writer = csv.DictWriter(csvfile, delimiter=';', fieldnames=common.map_band_fieldnames, lineterminator='\n')
        writer.writeheader()

        summary = summarize.read_csv_summary(inputfile)
        for row in summary[1:]: # ignore first row, which is a header
            INTERNET_ARCHIVE_ID_COL = 0
            BAND_NAME_COL = 1
            internet_archive_id_path = row[INTERNET_ARCHIVE_ID_COL]
            internet_archive_id = internet_archive_id_path.split("/")[-1]
            print "internet_archive_id:", internet_archive_id
            wikidata_object_response = retrieve_wikidata_object_by_internet_archive_id(internet_archive_id)
            print wikidata_object_response
            try:
                wikidata_object_response_json = json.loads(wikidata_object_response.content)
                items = wikidata_object_response_json[ITEMS_JSON]
                wikidata_band_id = 0
                musicbrainz_id = 0
                if len(items) > 0:
                    wikidata_band_id = items[0]
                    print 'wikidata_band_id:', wikidata_band_id
                    wikidata_band_response_json = common.is_stored_as_json_file(
                        WIKIDATA_API_URL + ITEMS_JSON + '[' + str(wikidata_band_id) + ']&' + PROPS_JSON + '=*')
                    if(wikidata_band_response_json == None):
                        print 'band data not exists for id:', wikidata_band_id
                        band_data_response = retrieve_wikidata_band_data(wikidata_band_id)
                        wikidata_band_data_response_json = common.validate_response_json(band_data_response)
                    store_wikidata_band_data(wikidata_band_id, wikidata_band_data_response_json)
                    print 'band json:', wikidata_band_data_response_json

                    try:
                        musicbrainz_id = extract_property_value(wikidata_band_data_response_json, MUSIC_BRAINZ_ARTIST_ID_PROP)
                    except:
                        print 'No musicbrainz id found for band ID:', wikidata_band_id

                    print 'musicbrainz id:', musicbrainz_id

                entry = build_band_mapping_entry(
                            row[BAND_NAME_COL]
                            , wikidata_band_id
                            , internet_archive_id
                            , musicbrainz_id
                )
                writer.writerow(entry)
            except KeyError as ke:
                print 'no composition items found:', row[INTERNET_ARCHIVE_ID_COL], ke


def extract_viaf_id_from_wikidata_composition_id(response):

    try:
        return response[ITEMS_JSON][0]
    except JSONDecodeError as jde:
        print 'JSONDecodeError. Response composition:', response.content
        print 'Incorrect JSON syntax!', jde
    except IndexError as ie:
        print 'No items found!', ie
    return None


def build_composition_mapping_entry(
        title, author_name, wikidata_id, viaf_id, musicbrainz_id):

    values = [
        title
        , author_name
        , wikidata_id
        , viaf_id
        , musicbrainz_id
    ]

    return dict(zip(common.map_compositions_fieldnames, values))


def build_band_mapping_entry(
        title, wikidata_id, internet_archive_id, musicbrainz_id):

    values = [
        title
        , wikidata_id
        , internet_archive_id
        , musicbrainz_id
    ]

    return dict(zip(common.map_band_fieldnames, values))


#def retrieve_wikidata_composition_data(wikidata_composition_id):

#    query_composition = WIKIDATA_API_URL + ITEMS_JSON + '[' + str(wikidata_composition_id) + ']&' + \
#                   PROPS_JSON + '=*'
#    print 'query composition:', query_composition
#    composition_response_json = common.process_http_query(query_composition)
#    print 'composition json data:', composition_response_json
#    return composition_response_json


def retrieve_wikidata_band_data(wikidata_band_id):

    query_band = WIKIDATA_API_URL + ITEMS_JSON + '[' + str(wikidata_band_id) + ']&' + \
                   PROPS_JSON + '=*'
    print 'query band:', query_band
    band_response_json = common.process_http_query(query_band)
    print 'band json data:', band_response_json
    return band_response_json


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
        return row[GND_COL].split(common.SLASH)[-1]
    except IndexError as ie:
        print 'No GND found!', ie
    return None


def get_wikidata_author_id_by_gnd(gnd, line):

    row = line.split(";")
    wikidata_author_id_response_json = common.is_stored_as_json_file(
        WIKIDATA_AUTHOR_DIR + common.SLASH + row[ONB_COL] + common.UNDERSCORE + gnd + '*')
    if(wikidata_author_id_response_json == None):
        print 'onb_wikidata not exists for ONB:', row[ONB_COL]
        wikidata_author_id_response = retrieve_wikidata_author_id(gnd)
        wikidata_author_id_response_json = wikidata_author_id_response.json()
    wikidata_author_id = extract_wikidata_author_id(wikidata_author_id_response_json)
    print 'wikidata_author_id', wikidata_author_id
    store_wikidata_author_id(line, wikidata_author_id, gnd, wikidata_author_id_response_json)
    return wikidata_author_id


def search_europeana_facets(url):

    print 'search Europeana facets - url:', url
    europeana_response = common.process_http_query(url)
    print 'response content:', europeana_response.content
    return europeana_response


def extract_and_save_label_data(europeana_response_json):

    fields = []
    try:
        fields = europeana_response_json[FACETS_JSON][0]
        with codecs.open(FACET_COLLECTION_FILE, 'w') as csvfile:
            writer = csv.DictWriter(csvfile, delimiter=';', fieldnames=facet_collection_fieldnames, lineterminator='\n')
            writer.writeheader()
        for field in fields[FIELDS_JSON]:
            label = field[LABEL_JSON]
            count = field[COUNT_JSON]
            with open(FACET_COLLECTION_FILE, 'ab') as csvfile:
                writer = csv.DictWriter(csvfile, delimiter=';', fieldnames=facet_collection_fieldnames, lineterminator='\n')
                try:
                    id = ''
                    if '_' in label:
                        id = label.split('_')[0]
                    print 'label:', label, 'id:', id, 'count:', count
                    label_res = common.toByteStr(label)
                    values = [
                        str(id)
                        , label_res
                        , str(count)
                    ]
                    entry = dict(zip(facet_collection_fieldnames, values))
                    writer.writerow(entry)
                except UnicodeEncodeError as uee:
                    print 'UnicodeEncodeError. Writing data in CSV for europeana facet collection. label:', label, uee

    except JSONDecodeError as jde:
        print 'JSONDecodeError. Response europeana facet collection data:', jde
    except:
        print 'Response json:', europeana_response_json
        print 'Unexpected error:', sys.exc_info()[0]
    return fields


def search_europeana_facets():

    europeana_response = common.process_http_query(EUROPEANA_COLLECTION_URL)
    print 'response content:', europeana_response.content
    europeana_response_json = europeana_response.json()
    # facets -> fields -> label
    labels = extract_and_save_label_data(europeana_response_json)
    print 'labels len', len(labels)


# store Wikidata author ID response in format {wikidata-id}_{onb-id}.json
def store_wikidata_author_id(line, author_id, gnd, response):

    row = line.split(";")
    common.write_json_file(WIKIDATA_AUTHOR_DIR, str(row[ONB_COL]) + common.UNDERSCORE
                           + gnd + common.UNDERSCORE + str(author_id) + common.JSON_EXT, response)


# store Wikidata author data response in format {wikidata-id}.json
def store_wikidata_author_data(author_id, response):

    common.write_json_file(common.WIKIDATA_AUTHOR_DATA_DIR, str(author_id) + common.JSON_EXT, response)


# store Wikidata composition data response in format {wikidata-id}.json
def store_wikidata_composition_data(composition_id, response):

    common.write_json_file(WIKIDATA_COMPOSITION_DATA_DIR, str(composition_id) + common.JSON_EXT, response)


# store Wikidata band data response in format {wikidata-id}.json
def store_wikidata_band_data(band_id, response):

    common.write_json_file(WIKIDATA_BAND_DATA_DIR, str(band_id) + common.JSON_EXT, response)


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
                common.WIKIDATA_AUTHOR_DATA_DIR + common.SLASH + str(wikidata_author_id) + '*')
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
        writer = csv.DictWriter(csvfile, delimiter=';', fieldnames=common.wikidata_author_fieldnames, lineterminator='\n')
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
