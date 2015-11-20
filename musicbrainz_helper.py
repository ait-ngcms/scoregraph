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
import glob

MUSICBRAINZ_API_URL = 'http://musicbrainz.org/ws/2/'
MUSICBRAINZ_COMPOSITION_DIR = 'data/musicbrainz_composition_dir'
MUSICBRAINZ_WORKS_DIR = 'data/musicbrainz_works_dir'
MUSICBRAINZ_RECORDINGS_DIR = 'data/musicbrainz_recordings_dir'
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
                store_musicbrainz_composition_data(musicbrainz_composition_id, json_data, MUSICBRAINZ_COMPOSITION_DIR)
                store_mapping_composition_viafid_musicbranzid(viaf_id, musicbrainz_composition_id)
    except ValueError as ve:
        print 'Could not find JSON for given Musicbrainz composition.', composition_title, ve.message
    except Exception as e:
        print 'Could not find Musicbrainz composition.', composition_title, e.message

    return work_response


# e.g. http://musicbrainz.org/ws/2/artist/8d610e51-64b4-4654-b8df-064b0fb7a9d9?inc=aliases%20works%20recordings&fmt=json
# for Mahler, Gustav
def retrieve_musicbrainz_works_and_recordings_by_id(id, author, output_works, output_recordings):

    try:
        query_work = MUSICBRAINZ_API_URL + 'artist/' + id + '?inc=aliases%20works%20recordings&fmt=json'
#        query_work = MUSICBRAINZ_API_URL + 'artist/' + id + '?inc=aliases%20works%20recordings&client=apikey&fmt=json'
#http://api.acoustid.org/v2/lookup?client=ULjKruIg&meta=recordings+releasegroups+compress&duration=641&fingerprint=AQABz0qUkZK4oOfhL-CPc4e5C_wW2H2QH9uDL4cvoT8UNQ-eHtsE8cceeFJx-LiiHT-aPzhxoc-Opj_eI5d2hOFyMJRzfDk-QSsu7fBxqZDMHcfxPfDIoPWxv9C1o3y
        query_work = MUSICBRAINZ_API_URL + 'artist/' + id + '?inc=aliases%20works%20recordings&client=apikey&fmt=json'
        print 'query work:', query_work
        work_response = common.process_http_query(query_work)
        print 'musicbrainz composition:', work_response
        musicbrainz_composition_response_json = json.loads(work_response.content)
        retrieve_compositions(musicbrainz_composition_response_json[common.WORKS_JSON], author, output_works, MUSICBRAINZ_WORKS_DIR)
        retrieve_compositions(musicbrainz_composition_response_json[common.RECORDINGS_JSON], author, output_recordings, MUSICBRAINZ_RECORDINGS_DIR)
    except ValueError as ve:
        print 'Could not find JSON for given Musicbrainz composition.', id, ve.message
    except Exception as e:
        print 'Could not find Musicbrainz composition.', id, e.message

    return work_response


# e.g. http://musicbrainz.org/ws/2/artist/8d610e51-64b4-4654-b8df-064b0fb7a9d9?inc=aliases%20works%20recordings&fmt=json
# count version: http://musicbrainz.org/ws/2/work?artist=8d610e51-64b4-4654-b8df-064b0fb7a9d9&inc=aliases&fmt=json
# for Mahler, Gustav
def calculate_musicbrainz_works_and_recordings_by_id(id, author, output_file):

    try:
#        query_work = MUSICBRAINZ_API_URL + 'artist/' + id + '?inc=aliases%20works%20recordings&fmt=json'
        query_work = MUSICBRAINZ_API_URL + 'work?artist=' + id + '&inc=aliases&fmt=json'
        print 'query compositions:', query_work
        work_response = common.process_http_query(query_work)
        print 'musicbrainz composition:', work_response
        musicbrainz_composition_response_json = json.loads(work_response.content)
#        works_count = len(musicbrainz_composition_response_json[common.WORKS_JSON])
        compositions_count = str(musicbrainz_composition_response_json[common.WORK_COUNT_JSON])
        #recordings_count = len(musicbrainz_composition_response_json[common.RECORDINGS_JSON])
        #compositions_count = works_count + recordings_count
        print 'musicbrainz #composition:', compositions_count
        values = [
            id
            , common.toByteStr(author)
            , str(compositions_count)
        ]

        entry = dict(zip(common.musicbrainz_compositions_count_fieldnames, values))
        with open(output_file, 'ab') as csvfile:
            writer = csv.DictWriter(csvfile, delimiter=';', fieldnames=common.musicbrainz_compositions_count_fieldnames, lineterminator='\n')
            writer.writerow(entry)
    except ValueError as ve:
        print 'Could not find JSON for given Musicbrainz composition.', id, ve.message
    except Exception as e:
        print 'Could not find Musicbrainz composition.', id, e.message

    return work_response


def retrieve_compositions(works, author, output_file, dir):

    if len(works) > 0:
        for json_data in works:
            musicbrainz_composition_id = json_data[common.ID_JSON]
            #if str(musicbrainz_composition_id) + common.JSON_EXT not in os.listdir(MUSICBRAINZ_WORKS_DIR):
            print 'musicbrainz_composition_id:', musicbrainz_composition_id
            store_musicbrainz_composition_data(musicbrainz_composition_id, json_data, dir)
            store_composition_musicbrainz(musicbrainz_composition_id, json_data, author, output_file)


def store_composition_musicbrainz(id, json_data, author, output_file):

    alias_names = ''
    for name_list in json_data[common.ALIASES_JSON]:
        value = name_list[common.NAME_JSON]
        if alias_names=='':
            alias_names = value
        else:
            alias_names += ' ' + value

    values = [
        id
        , common.toByteStr(author)
        , common.toByteStr(json_data[common.TITLE_JSON])
        , common.toByteStr(alias_names)
    ]

    entry = dict(zip(common.musicbrainz_works_and_recordings_fieldnames, values))
    write_composition_in_csv_file(output_file, entry)


def write_composition_in_csv_file(outputfile, entry):

    with open(outputfile, 'ab') as csvfile:
        writer = csv.DictWriter(csvfile, delimiter=';', fieldnames=common.musicbrainz_works_and_recordings_fieldnames, lineterminator='\n')
        writer.writerow(entry)


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
def store_musicbrainz_composition_data(composition_id, response, dir):

    common.write_json_file(dir, str(composition_id) + common.JSON_EXT, response)

def calculate_musicbrainz_works_and_recordings_count(inputfile, output_compositions):

    # an input file contains mapped author data with musicbrainz author IDs
    summary = summarize.read_csv_summary(inputfile)
    outputfile = glob.glob(output_compositions)
    if not outputfile:
    #if not output_compositions:
        with codecs.open(output_compositions, 'w') as csvfile:
            writer = csv.DictWriter(csvfile, delimiter=';', fieldnames=common.musicbrainz_compositions_count_fieldnames, lineterminator='\n')
            writer.writeheader()

    for row in summary[1:]: # ignore first row, which is a header
        try:
            mapping_musicbrainz_id = row[common.MUSICBRAINZ_ID_COL]
            author_name = row[common.AUTHOR_NAME_COL]
            print 'author name:', author_name, 'mapping musicbrainz id:', mapping_musicbrainz_id
            # an input file contains author compositions count with musicbrainz author IDs and names
            author_compositions = summarize.read_csv_summary(output_compositions)
            isStored = False
            for count_row in author_compositions[1:]:
                musicbrainz_author_id = count_row[0]
                if mapping_musicbrainz_id == musicbrainz_author_id:
                    isStored = True
                    print 'is already stored.'
                    break
            if isStored == False and mapping_musicbrainz_id:
                calculate_musicbrainz_works_and_recordings_by_id(mapping_musicbrainz_id.split(' ')[0], author_name, output_compositions)
        except:
            print ''


def retrieve_musicbrainz_works_and_recordings(inputfile, output_works, output_recordings):

    # an input file contains mapped author data with musicbrainz author IDs
    summary = summarize.read_csv_summary(inputfile)
    with codecs.open(output_works, 'w') as csvfile:
        writer = csv.DictWriter(csvfile, delimiter=';', fieldnames=common.musicbrainz_works_and_recordings_fieldnames, lineterminator='\n')
        writer.writeheader()

    with codecs.open(output_recordings, 'w') as csvfile:
        writer2 = csv.DictWriter(csvfile, delimiter=';', fieldnames=common.musicbrainz_works_and_recordings_fieldnames, lineterminator='\n')
        writer2.writeheader()

    for row in summary[1:]: # ignore first row, which is a header
        try:
            musicbrainz_id = row[common.MUSICBRAINZ_ID_COL]
            author_name = row[common.AUTHOR_NAME_COL]
            print 'author name:', author_name, 'musicbrainz id:', musicbrainz_id
            retrieve_musicbrainz_works_and_recordings_by_id(musicbrainz_id, author_name, output_works, output_recordings)
        except:
            print ''



# Main mapping routine

def retrieve_musicbrainz_composition_data(inputfile):

    # an input file contains work titles from the VIAF repository
    summary = summarize.read_csv_summary(inputfile)
    if not os.path.exists(inputfile):
        with codecs.open(VIAF_MUSICBRAINZ_COMPOSITION_MAPPING_FILE, 'w') as csvfile:
            writer = csv.DictWriter(csvfile, delimiter=';', fieldnames=common.viaf_musicbrainz_compositions_mapping_fieldnames, lineterminator='\n')
            writer.writeheader()

    for row in summary[1:]: # ignore first row, which is a header
        author_name = row[common.VIAF_COMPOSITIONS_CSV_AUTHOR_ID_COL]
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
