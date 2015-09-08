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

import dbpedia_helper

SUMMARY_AUTHORS_FILE = 'summary_authors.csv'


def analyze(inputdir, dirnames):

    mode_raw = 'raw'
    mode_normalized = 'normalized'
    mode_enriched = 'enriched'
    raw_path = inputdir + '/' + mode_raw
    normalized_path = inputdir + '/' + mode_normalized
    enriched_path = inputdir + '/' + mode_enriched

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
            summarize.summarize_titles(normalized_files, inputdir + '/summary_titles.csv')
            summarize.summarize_authors(normalized_files, inputdir + '/' + SUMMARY_AUTHORS_FILE)
        else:
            print 'Error. ' + mode_enriched + ' folder is missing.'
    else:
        print 'Error. ' + mode_normalized + ' folder is missing.'

    #query_list = summarize.read_csv_summary(inputdir + '/' + SUMMARY_AUTHORS_FILE)
    #dbpedia_helper.analyze_authors_by_dbpedia(query_list)

#    query_list = summarize.read_summary(inputdir + '/summary_titles.csv')
#    dbpedia_helper.analyze_titles_by_dbpedia(query_list)

    print '+++ Analyzing completed +++'


# Main analyzing routine

def analyze_records(inputdir):
    print("Analyzing '" + inputdir + "' records.")

    for (dirpath, dirnames, filenames) in walk(inputdir):
        analyze(inputdir, dirnames)
        break


# Command line parsing

if __name__ == '__main__':

    parser = argparse.ArgumentParser(
                    description="Analyzing data for dataset statistics.")
    parser.add_argument('inputdir', type=str, #nargs='+',
                    help="Input files to be processed")

    if len(sys.argv) < 1:
        parser.print_help()
        sys.exit(1)

    args = parser.parse_args()
    analyze_records(args.inputdir)
