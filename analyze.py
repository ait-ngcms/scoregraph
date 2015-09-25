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

Invocation:
$ python analyze.py data
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

import wikidata_helper
import mediawiki_helper
import freebase_helper

import time


SUMMARY_TITLES_FILE = 'summary_titles.csv'
SUMMARY_AUTHORS_FILE = 'summary_authors.csv'
SUMMARY_AUTHORS_NEW_FILE = 'summary_authors-new.csv'
MAPPED_AUTHORS_FILE = 'mapped_authors.csv'


def analyze(inputdir, dirnames):

    mode_raw = 'raw'
    mode_normalized = 'normalized'
    mode_enriched = 'enriched'
    raw_path = inputdir + '/' + mode_raw
    normalized_path = inputdir + '/' + mode_normalized
    enriched_path = inputdir + '/' + mode_enriched

    # clean up directories
    #common.cleanup_tmp_directories(raw_path)
    #common.cleanup_tmp_directories(normalized_path)
    #common.cleanup_tmp_directories(enriched_path)
    #os.remove('/summary_' + mode_normalized + '.csv')
    #os.remove('/summary_' + mode_enriched + '.csv')
    #os.remove(SUMMARY_TITLES_FILE)
    #os.remove(SUMMARY_AUTHORS_FILE)

    # correct IDs in CSV
    #summarize.correct_authors(inputdir + '/' + SUMMARY_AUTHORS_FILE)

    '''
    # normalize entities
    if mode_raw in dirnames:
        raw_files = os.listdir(raw_path)
        raw_files = [(raw_path + '/' + element) for element in raw_files]
        normalize.normalize_records(raw_files, normalized_path)
    else:
        print 'Error. ' +  mode_raw + ' folder is missing.'

    # enrich entities with Europeana data using GND number
    if mode_normalized in dirnames:
        normalized_files = os.listdir(normalized_path)
        normalized_files = [(normalized_path + '/' + element) for element in normalized_files]
        enrich.enrich_records(normalized_files, enriched_path)

        # summarize statistics
        if mode_enriched in dirnames:
            summarize.summarize_records(normalized_files, inputdir + '/summary_' + mode_normalized + '.csv')
            enriched_files = os.listdir(enriched_path)
            enriched_files = [(enriched_path + '/' + element) for element in enriched_files]
            summarize.summarize_records(enriched_files, inputdir + '/summary_' + mode_enriched + '.csv')
            summarize.summarize_titles(normalized_files, inputdir + '/' + SUMMARY_TITLES_FILE)
            summarize.summarize_authors(normalized_files, inputdir + '/' + SUMMARY_AUTHORS_FILE)
        else:
            print 'Error. ' + mode_enriched + ' folder is missing.'
    else:
        print 'Error. ' + mode_normalized + ' folder is missing.'

#    query_list = summarize.read_summary(inputdir + '/summary_titles.csv')
#    dbpedia_helper.analyze_titles_by_dbpedia(query_list)
    '''


    '''
    # map entities employing Wikidata using GND number
    wikidata_helper.map_records(
        inputdir + '/' + SUMMARY_AUTHORS_NEW_FILE
        , inputdir + '/' + MAPPED_AUTHORS_FILE
    )
    '''

    '''
    # map entities employing MediaWiki API using GND number
    mediawiki_helper.map_records(
        inputdir + '/' + SUMMARY_AUTHORS_NEW_FILE
        , inputdir + '/' + MAPPED_AUTHORS_FILE
    )
    '''

    freebase_helper.summarize_categories()
    print '+++ Analyzing completed +++'



# Main analyzing routine

def analyze_records(inputdir):

    start = time.time()
    print("Analyzing '" + inputdir + "' records.")

    for (dirpath, dirnames, filenames) in walk(inputdir):
        analyze(inputdir, dirnames)
        break

    end = time.time()
    print 'Calculation time:', end - start


# Command line parsing

if __name__ == '__main__':

    parser = argparse.ArgumentParser(
                    description="Analyzing data for dataset statistics.")
    parser.add_argument('inputdir', type=str, help="Input files to be processed")

    if len(sys.argv) < 1:
        parser.print_help()
        sys.exit(1)

    args = parser.parse_args()
    analyze_records(args.inputdir)
