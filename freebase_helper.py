#!/usr/bin/env python
"""
Script for Freebase connection.

Invocation:
$ python freebase_helper.py query
"""

import argparse
import sys

import json
import urllib

import common
import glob

import csv
import codecs


COMPOSITIONS = "compositions"
FREEBASE_COMPOSITIONS_DIR = 'data/freebase_compositions_dir'
FREEBASE_COMPOSITIONS_DATA_DIR = 'data/freebase_compositions_data_dir'
FREEBASE_ID_PREFIX = '/m/'
SUMMARY_COMPOSITIONS_FILE = 'data/summary_compositions.csv'
AUTHOR_COMPOSITIONS_FILE = 'data/author_compositions.csv'
COMPOSITIONS_DATA_FILE = 'data/compositions_data.csv'


composition_fieldnames = [
    'freebase_author_id'
    , 'composition_count'
]


author_composition_fieldnames = [
    'author'
    , 'composition'
    , 'parent'
    , 'main'
]


composition_data_fieldnames = [
    'id'
    , 'mid'
    , 'name'
]


def retrieve_compositions(author_id):

    if author_id:
        query = [{'mid': author_id,
              'name': None,
              "type": "/music/composer",
              COMPOSITIONS: [{}]
             }]
        response = find_freebase_items(query)
        try:
            for author in response['result']:
                print author['name']
                print COMPOSITIONS, author[COMPOSITIONS]
                store_compositions(author_id, response)
        except KeyError as ke:
            print 'incorrect Freebase key:', author_id, ke


def save_mapping_authors_to_composition_count_in_csv(filename_authors, outputfile):

    reader = csv.DictReader(open(filename_authors), delimiter=';', fieldnames=common.viaf_compositions_count_fieldnames, lineterminator='\n')
    firstTime = True
    for row in reader:
        if not firstTime:
            print 'row', row
            author = row[common.AUTHOR_NAME]
            author_id = author.split('.')[0]
            name, length = count_compositions(author_id)
            if name != None:
                print 'author:', name, 'len compositions', length
                entry = build_freebase_composition_count_entry(common.toByteStr(name), length)
                write_composition_in_csv_file(outputfile, entry)
        else:
            firstTime = False


def build_freebase_composition_count_entry(
        author_name, length):

    values = [
        author_name
        , length
    ]

    return dict(zip(common.viaf_compositions_count_fieldnames, values))


def write_composition_in_csv_file(outputfile, entry):

    with open(outputfile, 'ab') as csvfile:
        writer = csv.DictWriter(csvfile, delimiter=';', fieldnames=common.viaf_compositions_count_fieldnames, lineterminator='\n')
        writer.writerow(entry)


def count_compositions(author_id):

    if author_id:
        query = [{'mid': common.FREEBASE_PREFIX + author_id,
              common.AUTHOR_NAME_HEADER: None,
              "type": "/music/composer",
              COMPOSITIONS: [{"return": "count"}]
             }]
        response = find_freebase_items(query)
        try:
            if any(response[common.RESULT]) == False:
                return None, None
            for author in response[common.RESULT]:
                print author[common.AUTHOR_NAME_HEADER]
                print COMPOSITIONS, author[COMPOSITIONS]
                count = 0
                if author[COMPOSITIONS][0]:
                    count = author[COMPOSITIONS][0]
                return author[common.AUTHOR_NAME_HEADER], count
        except KeyError as ke:
            print 'incorrect Freebase key:', author_id, ke


def retrieve_compositions_data(composition_id):

    if composition_id:
        query = [{'id': composition_id,
                  'mid': None,
                  'name': None,
                  "type": [{}],
                  "i18n:name": [{}],
                  "key": [{}]
             }]
        response = find_freebase_items(query)
        try:
            for composition in response['result']:
                print composition['name']
                store_compositions_data(composition['mid'], response)
        except KeyError as ke:
            print 'incorrect Freebase key:', composition_id, ke
            response = None
    return response


def store_compositions(author_id, response):

    filename = str(author_id).replace(FREEBASE_ID_PREFIX,'') + common.JSON_EXT
    response_json = common.is_stored_as_json_file(FREEBASE_COMPOSITIONS_DIR + common.SLASH + filename)
    if(response_json == None):
    #inputfile = glob.glob(FREEBASE_COMPOSITIONS_DIR + SLASH + filename)
    #if not inputfile:
        print 'composition not exists for author:', author_id
        common.write_json_file(FREEBASE_COMPOSITIONS_DIR, filename, response)


def store_compositions_data(composition_id, response):

    filename = str(composition_id).replace(FREEBASE_ID_PREFIX,'') + common.JSON_EXT
    response_json = common.is_stored_as_json_file(FREEBASE_COMPOSITIONS_DATA_DIR + common.SLASH + filename)
    if(response_json == None):
    #inputfile = glob.glob(FREEBASE_COMPOSITIONS_DATA_DIR + SLASH + filename)
    #if not inputfile:
        print 'composition not exists for ID:', composition_id
        common.write_json_file(FREEBASE_COMPOSITIONS_DATA_DIR, filename, response)


# Main query routine

def find_freebase_items(query):

    api_key = open("freebase_api_key").read()
    service_url = 'https://www.googleapis.com/freebase/v1/mqlread'
    params = {
            'query': json.dumps(query),
            'key': api_key
    }
    url = service_url + '?' + urllib.urlencode(params)
    response = json.loads(urllib.urlopen(url).read())
    return response


def summarize_compositions():

    with codecs.open(SUMMARY_COMPOSITIONS_FILE, 'w') as csvfile:
        writer = csv.DictWriter(csvfile, delimiter=';', fieldnames=composition_fieldnames, lineterminator='\n')
        writer.writeheader()

        for inputfile in glob.glob(FREEBASE_COMPOSITIONS_DIR + common.SLASH + '*'):
            print inputfile
            compositions_content_json = common.read_json_file(inputfile)
            #compositions_content_json = json.loads(compositions_content)
            composition_list = compositions_content_json['result'][0]['compositions']
            entry = build_composition_entry(inputfile.split('\\')[-1], len(composition_list))
            writer.writerow(entry)


def get_composition_string_list_from_json_list(composition_json_list):

    composition_list = []
    for composition_json in composition_json_list:
        if composition_json['name'] != None:
            composition_str = common.toByteStr(composition_json['name']).lower()
            if composition_str != None:
                composition_list.append(composition_str)
    return sorted(composition_list)


def get_composition_id_list_from_json_list(composition_json_list):

    composition_list = []
    for composition_json in composition_json_list:
        if composition_json['id'] != None:
            composition_str = common.toByteStr(composition_json['id']).lower()
            if composition_str != None:
                composition_list.append(composition_str)
    return sorted(composition_list)


def analyze_compositions():

    with codecs.open(AUTHOR_COMPOSITIONS_FILE, 'w') as csvfile:
        writer = csv.DictWriter(csvfile, delimiter=';', fieldnames=author_composition_fieldnames, lineterminator='\n')
        writer.writeheader()

        for inputfile in glob.glob(FREEBASE_COMPOSITIONS_DIR + common.SLASH + '*'):
            print inputfile
            compositions_content_json = common.read_json_file(inputfile)
            name = compositions_content_json['result'][0]['name']
            composition_json_list = compositions_content_json['result'][0]['compositions']
            composition_list = get_composition_string_list_from_json_list(composition_json_list)

            if len(composition_list) > 0:
#                parent = assign_parent(composition_list[0])

                parent = ''
                for index, composition in enumerate(composition_list):
                    main = composition
                    if index == 0:
                        parent = assign_parent(composition_list[0])
                    else:
                        #if parent not in composition:
                        if not composition.startswith(parent):
                            parent = assign_parent(composition)
                        else:
                            parent_new = common.find_common_substring(parent,composition_list[index-1])
                            #parent_new = common.find_common_parent(parent,composition_list[index-1])
                            # parent ending must be either ' ' or ','
                            if parent_new != '':
                                print 'parent:', parent, 'parent_new:', parent_new, 'composition:', composition
                                if (len(parent_new) <= len(composition)
                                    and composition[len(parent_new)-1] != ' ' \
                                    and composition[len(parent_new)-1] != ','):
                                    parent_new = composition
                                parent = parent_new
                    entry = build_author_composition_entry(common.toByteStr(name), composition, parent, main)
                    writer.writerow(entry)


def assign_parent(value):

    parent = value
    if ',' in parent:
        posComma = parent.index(',')
        parent = parent[:posComma]
    #parent = common.check_parent_at_least_a_word(parent, parent_new)
    return parent


def build_composition_entry(
        freebase_author_id, composition_count):

    values = [
        freebase_author_id
        , composition_count
    ]

    return dict(zip(composition_fieldnames, values))


def build_author_composition_entry(
        author, composition, parent, main):

    values = [
        author
        , composition
        , parent
        , main
    ]

    return dict(zip(author_composition_fieldnames, values))


def build_composition_data_entry(
        id, mid, name):

    values = [
        id
        , mid
        , name
    ]

    return dict(zip(composition_data_fieldnames, values))


def aggregate_compositions_data():

    with codecs.open(COMPOSITIONS_DATA_FILE, 'w') as csvfile:
        writer = csv.DictWriter(csvfile, delimiter=';', fieldnames=composition_data_fieldnames, lineterminator='\n')
        writer.writeheader()

        for inputfile in glob.glob(FREEBASE_COMPOSITIONS_DIR + common.SLASH + '*'):
            print inputfile
            compositions_content_json = common.read_json_file(inputfile)
            composition_json_list = compositions_content_json['result'][0]['compositions']
            composition_list = get_composition_id_list_from_json_list(composition_json_list)

            if len(composition_list) > 0:
                for index, composition_id in enumerate(composition_list):
                    composition_data = retrieve_compositions_data(composition_id)
                    if composition_data:
                        try:
                            mid = composition_data['result'][0]['mid']
                            name = composition_data['result'][0]['name']
                            entry = build_composition_data_entry(composition_id, mid, common.toByteStr(name))
                            writer.writerow(entry)
                        except:
                            print 'Composition values mid and/or name is empty.'



# Command line parsing

if __name__ == '__main__':

    parser = argparse.ArgumentParser(
                    description="Freebase query.")
    parser.add_argument('query', type=str, nargs='?',
                    help="Input query text to be processed")

    if len(sys.argv) < 1:
        parser.print_help()
        sys.exit(1)

    args = parser.parse_args()
    retrieve_compositions(args.query)
