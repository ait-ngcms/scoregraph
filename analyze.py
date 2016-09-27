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
import musicbrainz_helper
import statistics

import time


SUMMARY_TITLES_FILE = 'summary_titles.csv'
SUMMARY_SAMEAS_FILE = 'summary_sameas.csv'
SUMMARY_AUTHORS_FILE = 'summary_authors.csv'
SUMMARY_AUTHORS_NEW_FILE = 'summary_authors-new.csv'
MAPPED_AUTHORS_FILE = 'mapped_authors.csv'
VIAF_COMPOSITIONS_FILE = 'viaf_compositions.csv'
VIAF_COMPOSITIONS_COUNT_FILE = 'viaf_compositions_count.csv'
FREEBASE_COMPOSITIONS_COUNT_FILE = 'freebase_compositions_count.csv'
COMPREHENSIVE_COMPOSITIONS_COUNT_FILE = 'comprehensive_compositions_count.csv'
MUSICBRAINZ_WORKS_FILE = 'musicbrainz_works.csv'
MUSICBRAINZ_RECORDINGS_FILE = 'musicbrainz_recordings.csv'
MAPPED_COMPOSITIONS_FILE = 'mapped_compositions.csv'
MUSICBRAINZ_COMPOSITIONS_COUNT_FILE = 'musicbrainz_compositions_count.csv'
BAND_INPUT_FILE = 'bands.csv'
MAPPED_BAND_FILE = 'mapped_bands.csv'


NORMALIZE = 'normalize'
ENRICH = 'enrich'
SAME_AS = 'same_as'
SUMMARIZE_AUTHORS = 'summarize_authors'
SUMMARIZE_TITLES = 'summarize titles'
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
STORE_JSON_WIKIDATA_AUTHOR_DATA_IN_NEO4J = 'store_json_wikidata_author_data_in_neo4j'
SEARCH_IN_JSON_NEO4J = 'search_in_json_neo4j'
GET_EUROPEANA_FACETS_COLLECTION = 'get_europeana_facets_collection'
SAVE_MAPPING_VIAF_AUTHOR_COMPOSITIONS_IN_CSV = 'save_mapping_viaf_author_compositions_in_csv'
SAVE_MAPPING_FREEBASE_AUTHOR_COMPOSITIONS_IN_CSV = 'save_mapping_freebase_author_compositions_in_csv'
RETRIEVE_MUSICBRAINZ_COMPOSITION_DATA = 'retrieve_musicbrainz_composition_data'
RETRIEVE_VIAF_COMPOSITION_DATA = 'retrieve_viaf_composition_data'
COMPREHENSIVE_COMPOSITION_STATISTIC = 'comprehensive_composition_statistic'
RETRIEVE_MUSICBRAINZ_WORKS_AND_RECORDINGS = 'retrieve_musicbrainz_works_and_recordings'
MAP_COMPOSITION_DATA_IN_CSV = 'map_composition_data_in_csv'
CALCULATE_MUSICBRAINZ_WORKS_AND_RECORDINGS_COUNT = 'calculate_musicbrainz_works_and_recordings_count'
MAP_BAND_DATA_IN_CSV = 'map_band_data_in_csv'


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

    if use_case == SAME_AS:
        # summarize sameAs entries in enriched JSON
        enriched_files = os.listdir(enriched_path)
        enriched_files = [(enriched_path + common.SLASH + element) for element in enriched_files]
        if mode_enriched in dirnames:
            summarize.summarize_sameas(enriched_files, inputdir + common.SLASH + SUMMARY_SAMEAS_FILE)
        else:
            print 'Error. ' + mode_enriched + ' folder is missing.'

    if use_case == SUMMARIZE_TITLES:
        if mode_normalized in dirnames:
            normalized_files = os.listdir(normalized_path)
            normalized_files = [(normalized_path + common.SLASH + element) for element in normalized_files]

            # summarize statistics
            if mode_enriched in dirnames:
                summarize.summarize_titles(normalized_files, inputdir + common.SLASH + SUMMARY_TITLES_FILE)
            else:
                print 'Error. ' + mode_enriched + ' folder is missing.'
        else:
            print 'Error. ' + mode_normalized + ' folder is missing.'

    if use_case == SUMMARIZE_AUTHORS:
        if mode_normalized in dirnames:
            normalized_files = os.listdir(normalized_path)
            normalized_files = [(normalized_path + common.SLASH + element) for element in normalized_files]

            # summarize statistics
            if mode_enriched in dirnames:
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
        neo4j_manager.save_mapped_authors_from_csv(inputdir + common.SLASH + MAPPED_AUTHORS_FILE
                                                    , inputdir + common.SLASH + VIAF_COMPOSITIONS_FILE)

    if use_case == STORE_JSON_WIKIDATA_AUTHOR_DATA_IN_NEO4J:
        neo4j_manager.save_json_wikidata_author_data_dir(common.WIKIDATA_AUTHOR_DATA_DIR)

    if use_case == SEARCH_IN_JSON_NEO4J:
        neo4j_manager.search_in_json_neo4j('1268')

    if use_case == GET_EUROPEANA_FACETS_COLLECTION:
        wikidata_helper.search_europeana_facets()

    if use_case == SAVE_MAPPING_VIAF_AUTHOR_COMPOSITIONS_IN_CSV:
        neo4j_manager.save_mapping_viaf_authors_to_composition_count_in_csv(inputdir + common.SLASH + MAPPED_AUTHORS_FILE
                                                    , inputdir + common.SLASH + VIAF_COMPOSITIONS_FILE
                                                    , inputdir + common.SLASH + VIAF_COMPOSITIONS_COUNT_FILE)

    if use_case == SAVE_MAPPING_FREEBASE_AUTHOR_COMPOSITIONS_IN_CSV:
        freebase_helper.save_mapping_authors_to_composition_count_in_csv(freebase_helper.SUMMARY_COMPOSITIONS_FILE
                                                    , inputdir + common.SLASH + FREEBASE_COMPOSITIONS_COUNT_FILE)

    if use_case == RETRIEVE_MUSICBRAINZ_COMPOSITION_DATA:
        musicbrainz_helper.retrieve_musicbrainz_composition_data(inputdir + common.SLASH + VIAF_COMPOSITIONS_FILE)

    if use_case == RETRIEVE_MUSICBRAINZ_WORKS_AND_RECORDINGS:
        musicbrainz_helper.retrieve_musicbrainz_works_and_recordings(
            inputdir + common.SLASH + MAPPED_AUTHORS_FILE
            , inputdir + common.SLASH + MUSICBRAINZ_WORKS_FILE
            , inputdir + common.SLASH + MUSICBRAINZ_RECORDINGS_FILE
        )

    if use_case == RETRIEVE_VIAF_COMPOSITION_DATA:
        viaf_helper.retrieve_viaf_composition_data(inputdir + common.SLASH + VIAF_COMPOSITIONS_FILE)

    if use_case == COMPREHENSIVE_COMPOSITION_STATISTIC:
        statistics.retrieve_comprehensive_composition_count(
            inputdir + common.SLASH + MAPPED_AUTHORS_FILE
            , inputdir + common.SLASH + VIAF_COMPOSITIONS_FILE
            , inputdir + common.SLASH + SUMMARY_SAMEAS_FILE
            , inputdir + common.SLASH + COMPREHENSIVE_COMPOSITIONS_COUNT_FILE
        )

    if use_case == MAP_COMPOSITION_DATA_IN_CSV:
        statistics.map_composition_data_in_csv(
            inputdir + common.SLASH + MUSICBRAINZ_WORKS_FILE
            , inputdir + common.SLASH + MAPPED_COMPOSITIONS_FILE)

    if use_case == CALCULATE_MUSICBRAINZ_WORKS_AND_RECORDINGS_COUNT:
        musicbrainz_helper.calculate_musicbrainz_works_and_recordings_count(
            inputdir + common.SLASH + MAPPED_AUTHORS_FILE
            , inputdir + common.SLASH + MUSICBRAINZ_COMPOSITIONS_COUNT_FILE
        )

    if use_case == MAP_BAND_DATA_IN_CSV:
        statistics.map_band_data_in_csv(
            inputdir + common.SLASH + BAND_INPUT_FILE
            , inputdir + common.SLASH + MAPPED_BAND_FILE)

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
                    help="Analysis use cases in given order, such as 'normalize', 'enrich', 'same_as', 'dbpedia_analysis', 'wikidata_map'"
                         ", 'mediawiki_map', 'summarize_compositions', 'analyze_compositions'"
                         ", 'aggregate_compositions_data', 'retrieve_wikidata_compositions'"
                         ", 'retrieve_viaf_data', 'load_mediawiki_properties', 'store_data_in_neo4j'"
                         ", 'store_json_wikidata_author_data_in_neo4j', 'search_in_json_neo4j'"
                         ", 'get_europeana_facets_collection', 'save_mapping_viaf_author_compositions_in_csv'"
                         ", 'save_mapping_freebase_author_compositions_in_csv', 'retrieve_musicbrainz_composition_data'"
                         ", 'retrieve_viaf_composition_data', 'comprehensive_composition_statistic', 'summarize_authors'"
                         ", 'summarize_titles', 'retrieve_musicbrainz_works_and_recordings', 'map_composition_data_in_csv', 'map_band_data_in_csv', 'cleanup'")

    if len(sys.argv) < 2:
        parser.print_help()
        sys.exit(1)

    args = parser.parse_args()
    analyze_records(args.inputdir, args.use_case)
