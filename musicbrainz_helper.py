#!/usr/bin/env python
"""
Script for summarizing data for statistics generation.

Invocation:
$ python musicbrainz_helper.py data/viaf_compositions.csv
"""

import argparse
import csv
import sys
import common
import summarize
import json
import codecs
import os

MUSICBRAINZ_API_URL = 'http://musicbrainz.org/ws/2/'
MUSICBRAINZ_COMPOSITION_DIR = 'data/musicbrainz_composition_dir'
VIAF_MUSICBRAINZ_COMPOSITION_MAPPING_FILE = 'data/musicbrainz_viaf_composition_mapping.csv'


# e.g. http://musicbrainz.org/ws/2/work/?query=Des Antonius von Padua Fischpredigt Voix, orchestre&fmt=json
def retrieve_musicbrainz_compositions_by_title(composition_title, viaf_id):

    try:
        query_work = MUSICBRAINZ_API_URL + 'work/?query=' + composition_title + '&fmt=json'
        print 'query work:', query_work
        work_response = common.process_http_query(query_work)
        print 'musicbrainz composition:', work_response
        musicbrainz_composition_response_json = json.loads(work_response.content)
        works = musicbrainz_composition_response_json[common.WORKS_JSON]
        if len(works) > 0:
            json_data = works[0]
            musicbrainz_composition_id = json_data[common.ID_JSON]
            if str(musicbrainz_composition_id) + common.JSON_EXT not in os.listdir(MUSICBRAINZ_COMPOSITION_DIR):
                print 'musicbrainz_composition_id:', musicbrainz_composition_id
                store_musicbrainz_composition_data(musicbrainz_composition_id, json_data)
                store_mapping_composition_viafid_musicbranzid(viaf_id, musicbrainz_composition_id)
    except ValueError as ve:
        print 'Could not find JSON for given Musicbrainz composition.', composition_title, ve.message
    except Exception as e:
        print 'Could not find Musicbrainz composition.', composition_title, e.message

    return work_response


def store_mapping_composition_viafid_musicbranzid(viaf_id, musicbrainz_composition_id):

    entry = build_mapping_composition_entry(viaf_id, musicbrainz_composition_id)
    write_composition_mapping_in_csv_file(VIAF_MUSICBRAINZ_COMPOSITION_MAPPING_FILE, entry)


def build_mapping_composition_entry(
        viaf_id, musicbrainz_composition_id):

    values = [
        viaf_id
        , musicbrainz_composition_id
    ]

    return dict(zip(common.viaf_musicbrainz_compositions_mapping_fieldnames, values))


def write_composition_mapping_in_csv_file(outputfile, entry):

    with open(outputfile, 'ab') as csvfile:
        writer = csv.DictWriter(csvfile, delimiter=';', fieldnames=common.viaf_musicbrainz_compositions_mapping_fieldnames, lineterminator='\n')
        writer.writerow(entry)


# store Musicbrainz composition data response in format {composition-id}.json
def store_musicbrainz_composition_data(composition_id, response):

    common.write_json_file(MUSICBRAINZ_COMPOSITION_DIR, str(composition_id) + common.JSON_EXT, response)


# Main mapping routine

def retrieve_musicbrainz_composition_data(inputfile):

    # an input file contains work titles from the VIAF repository
    summary = summarize.read_csv_summary(inputfile)
    if not os.path.exists(inputfile):
        with codecs.open(VIAF_MUSICBRAINZ_COMPOSITION_MAPPING_FILE, 'w') as csvfile:
            writer = csv.DictWriter(csvfile, delimiter=';', fieldnames=common.viaf_musicbrainz_compositions_mapping_fieldnames, lineterminator='\n')
            writer.writeheader()

    for row in summary[1:]: # ignore first row, which is a header
        author_name = row[common.AUTHOR_NAME_COL]
        composition_title = row[common.VIAF_COMPOSITIONS_CSV_COMPOSITION_TITLE_COL]
        viaf_id = row[common.VIAF_COMPOSITIONS_CSV_VIAF_WORK_ID_COL].replace('VIAF|','')
        print 'author name:', author_name, 'composition title:', composition_title, 'viaf ID:', viaf_id
        retrieve_musicbrainz_compositions_by_title(composition_title, viaf_id)


# Command line parsing

if __name__ == '__main__':

    parser = argparse.ArgumentParser(
                    description="Mapping identifiers and data for dataset compositions. Musicbrainz query.")
    parser.add_argument('inputfile', type=str, nargs='+',
                    default="data/musicbrainz_compositions.csv",
                    help="Input file to be processed")


    if len(sys.argv) < 1:
        parser.print_help()
        sys.exit(1)

    args = parser.parse_args()
    retrieve_musicbrainz_composition_data(args.inputfile, args.outputfile)
