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
FREEBASE_ID_PREFIX = '/m/'
SLASH = '/'
SUMMARY_COMPOSITIONS_FILE = 'data/summary_compositions.csv'
AUTHOR_COMPOSITIONS_FILE = 'data/author_compositions.csv'


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


def store_compositions(author_id, response):

    filename = str(author_id).replace(FREEBASE_ID_PREFIX,'') #('/','%2F')
    inputfile = glob.glob(FREEBASE_COMPOSITIONS_DIR + SLASH + filename)
    if not inputfile:
        print 'composition not exists for author:', author_id
        common.write_json_file(FREEBASE_COMPOSITIONS_DIR, filename, response)


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


def summarize_categories():

    with codecs.open(SUMMARY_COMPOSITIONS_FILE, 'w') as csvfile:
        writer = csv.DictWriter(csvfile, delimiter=';', fieldnames=composition_fieldnames, lineterminator='\n')
        writer.writeheader()

        for inputfile in glob.glob(FREEBASE_COMPOSITIONS_DIR + SLASH + '*'):
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
    return sorted(composition_list) #.sort()


def analyze_categories():

    with codecs.open(AUTHOR_COMPOSITIONS_FILE, 'w') as csvfile:
        writer = csv.DictWriter(csvfile, delimiter=';', fieldnames=author_composition_fieldnames, lineterminator='\n')
        writer.writeheader()

        for inputfile in glob.glob(FREEBASE_COMPOSITIONS_DIR + SLASH + '*'):
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
                        if parent not in composition:
                            parent = assign_parent(composition)
                        #if index + 1 < len(composition_list):
                        parent_new = common.find_common_substring(parent,composition_list[index-1])
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
