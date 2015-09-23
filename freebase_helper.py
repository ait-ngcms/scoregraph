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


COMPOSITIONS = "compositions"
FREEBASE_COMPOSITIONS_DIR = 'data/freebase_compositions_dir'
FREEBASE_ID_PREFIX = '/m/'
SLASH = '/'


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
