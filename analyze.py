#!/usr/bin/env python
"""
Script for analyzing data for statistics generation.

Data to analyze is comprised in the folder 'data' that should have subdirectories:
    'raw'        for initial XML files                          (input data)
    'normalized' that contains normalized data in JSON format   (output data)
    'enhanced'   that contains data enhanced by analyzing       (output data)

Statistics are generated in:
    summary_normalized.csv
    summary_enriched.csv
    ...

Invocation:
$ python analyze.py data -u use_case
"""

import argparse
import os
import sys

# directory structure
from os import walk

# parts of analysis
import normalize
import enrich
import summarize
import common

import dbpedia_helper
import wikidata_helper
import mediawiki_helper
import freebase_helper
import viaf_helper
import neo4j_manager

import time


SUMMARY_TITLES_FILE = 'summary_titles.csv'
SUMMARY_AUTHORS_FILE = 'summary_authors.csv'
SUMMARY_AUTHORS_NEW_FILE = 'summary_authors-new.csv'
MAPPED_AUTHORS_FILE = 'mapped_authors.csv'
VIAF_COMPOSITIONS_FILE = 'viaf_compositions.csv'

NORMALIZE = 'normalize'
ENRICH = 'enrich'
DBPEDIA_ANALYSIS = 'dbpedia_analysis'
WIKIDATA_MAP = 'wikidata_map'
MEIDAWIKI_MAP = 'mediawiki_map'
SUMMARIZE_COMPOSITIONS = 'summarize_compositions'
ANALYZE_COMPOSITIONS = 'analyze_compositions'
AGGREGATE_COMPOSITIONS_DATA = 'aggregate_compositions_data'
RETRIEVE_WIKIDATA_COMPOSITIONS = 'retrieve_wikidata_compositions'
RETRIEVE_VIAF_DATA = 'retrieve_viaf_data'
LOAD_MEDIAWIKI_PROPERTIES = 'load_mediawiki_properties'
CLEANUP = 'cleanup'
STORE_DATA_IN_NEO4J = 'store_data_in_neo4j'


def analyze(inputdir, dirnames, use_case):

    mode_raw = 'raw'
    mode_normalized = 'normalized'
    mode_enriched = 'enriched'
    raw_path = inputdir + common.SLASH + mode_raw
    normalized_path = inputdir + common.SLASH + mode_normalized
    enriched_path = inputdir + common.SLASH + mode_enriched

    if use_case == CLEANUP:
        # clean up directories
        common.cleanup_tmp_directories(raw_path)
        common.cleanup_tmp_directories(normalized_path)
        common.cleanup_tmp_directories(enriched_path)
        os.remove('/summary_' + mode_normalized + '.csv')
        os.remove('/summary_' + mode_enriched + '.csv')
        os.remove(SUMMARY_TITLES_FILE)
        os.remove(SUMMARY_AUTHORS_FILE)

    # correct IDs in CSV
    #summarize.correct_authors(inputdir + common.SLASH + SUMMARY_AUTHORS_FILE)

    if use_case == NORMALIZE:
        # normalize entities
        if mode_raw in dirnames:
            raw_files = os.listdir(raw_path)
            raw_files = [(raw_path + common.SLASH + element) for element in raw_files]
            normalize.normalize_records(raw_files, normalized_path)
        else:
            print 'Error. ' +  mode_raw + ' folder is missing.'

    if use_case == ENRICH:
        # enrich entities with Europeana data using GND number
        if mode_normalized in dirnames:
            normalized_files = os.listdir(normalized_path)
            normalized_files = [(normalized_path + common.SLASH + element) for element in normalized_files]
            enrich.enrich_records(normalized_files, enriched_path)

            # summarize statistics
            if mode_enriched in dirnames:
                summarize.summarize_records(normalized_files, inputdir + '/summary_' + mode_normalized + '.csv')
                enriched_files = os.listdir(enriched_path)
                enriched_files = [(enriched_path + common.SLASH + element) for element in enriched_files]
                summarize.summarize_records(enriched_files, inputdir + '/summary_' + mode_enriched + '.csv')
                summarize.summarize_titles(normalized_files, inputdir + common.SLASH + SUMMARY_TITLES_FILE)
                summarize.summarize_authors(normalized_files, inputdir + common.SLASH + SUMMARY_AUTHORS_FILE)
            else:
                print 'Error. ' + mode_enriched + ' folder is missing.'
        else:
            print 'Error. ' + mode_normalized + ' folder is missing.'

    if use_case == DBPEDIA_ANALYSIS:
        query_list = summarize.read_summary(inputdir + '/summary_titles.csv')
        dbpedia_helper.analyze_titles_by_dbpedia(query_list)


    if use_case == WIKIDATA_MAP:
        # map entities employing Wikidata using GND number
        wikidata_helper.map_records(
            inputdir + common.SLASH + SUMMARY_AUTHORS_NEW_FILE
            , inputdir + common.SLASH + MAPPED_AUTHORS_FILE
        )

    if use_case == MEIDAWIKI_MAP:
        # map entities employing MediaWiki API using GND number
        mediawiki_helper.map_records(
            inputdir + common.SLASH + SUMMARY_AUTHORS_NEW_FILE
            , inputdir + common.SLASH + MAPPED_AUTHORS_FILE
        )

    if use_case == SUMMARIZE_COMPOSITIONS:
        freebase_helper.summarize_compositions()

    if use_case == ANALYZE_COMPOSITIONS:
        freebase_helper.analyze_compositions()

    if use_case == AGGREGATE_COMPOSITIONS_DATA:
        freebase_helper.aggregate_compositions_data()

    if use_case == RETRIEVE_WIKIDATA_COMPOSITIONS:
        wikidata_helper.retrieve_wikidata_compositions_by_freebase_id(freebase_helper.COMPOSITIONS_DATA_FILE)

    if use_case == RETRIEVE_VIAF_DATA:
        viaf_helper.retrieve_authors_data_by_viaf_id(inputdir + common.SLASH + MAPPED_AUTHORS_FILE
                                                 , inputdir + common.SLASH + VIAF_COMPOSITIONS_FILE)
    if use_case == LOAD_MEDIAWIKI_PROPERTIES:
        mediawiki_helper.load_properties()

    if use_case == STORE_DATA_IN_NEO4J:
        neo_db = neo4j_manager.Neo4jManager()
        neo_db.remove_all_nodes()
        author_label = neo_db.create_label(neo4j_manager.AUTHOR_LABEL)
        values1 = [
            '1'
            , 'wiki id'
            , 'onb id'
            , 'Mahler, Gustav'
            , 'genres'
            , 'occupations'
            , 'freebase id'
            , 'viaf id'
            , 'bnf id'
            , 'nkc id'
            , 'nta id'
            , 'imslp id'
            , ''
            , 'music_brainz_artist id'
        ]
        author1 = dict(zip(common.wikidata_author_fieldnames, values1))
        an1 = neo_db.create_author(author_label, author1)
        values2 = [
            '2'
            , 'wiki id'
            , 'onb id'
            , 'Reitler, Josef'
            , 'genres'
            , 'occupations'
            , 'freebase id'
            , 'viaf id'
            , 'bnf id'
            , 'nkc id'
            , 'nta id'
            , 'imslp id'
            , ''
            , 'music_brainz_artist id'
        ]
        author2 = dict(zip(common.wikidata_author_fieldnames, values2))
        an2 = neo_db.create_author(author_label, author2)
        composition_label = neo_db.create_label(neo4j_manager.COMPOSITION_LABEL)
        c1 = neo_db.create_composition(composition_label, 'das klagende lied')
        neo_db.create_relationship(an1, c1, 'has_composition')
        composition_name = neo_db.query_composition_by_author_name(an1[neo4j_manager.NAME])
        print 'found composition', composition_name, 'for author', an1[neo4j_manager.NAME]

    print '+++ Analyzing completed +++'



# Main analyzing routine

def analyze_records(inputdir, use_case):

    start = time.time()
    print("Analyzing '" + inputdir + "' records for use case: " + use_case)

    for (dirpath, dirnames, filenames) in walk(inputdir):
        analyze(inputdir, dirnames, use_case)
        break

    end = time.time()
    print 'Calculation time:', end - start


# Command line parsing

if __name__ == '__main__':

    parser = argparse.ArgumentParser(
                    description="Analyzing data for dataset statistics.")
    parser.add_argument('inputdir', type=str, help="Input files to be processed")
    parser.add_argument('-u', '--use_case', type=str, nargs='?',
                    default="data/mapping.csv",
                    help="Analysis use cases in given order, such as 'normalize', 'enrich', 'dbpedia_analysis', 'wikidata_map'"
                         ", 'mediawiki_map', 'summarize_compositions', 'analyze_compositions'"
                         ", 'aggregate_compositions_data', 'retrieve_wikidata_compositions'"
                         ", 'retrieve_viaf_data', 'load_mediawiki_properties', 'store_data_in_neo4j', 'cleanup'")

    if len(sys.argv) < 2:
        parser.print_help()
        sys.exit(1)

    args = parser.parse_args()
    analyze_records(args.inputdir, args.use_case)
