#!/usr/bin/env python
"""
Script for summarizing data for statistics generation.

Invocation:
$ python statistics.py data/mapped_authors.csv -o data/viaf_compositions.csv
"""

import argparse
import csv
import sys
import codecs

import common

import summarize

import neo4j_manager
import freebase_helper
import enrich # connection to Europeana

import xml.etree.ElementTree as ET


def build_comprehensive_composition_count_entry(
        gnd, author_name, viaf_len, freebase_len, europeana_len):

    values = [
        gnd
        , author_name
        , viaf_len
        , freebase_len
        , europeana_len
    ]

    return dict(zip(common.comprehensive_compositions_count_fieldnames, values))



# Main mapping routine

def retrieve_comprehensive_composition_count(filename_authors, filename_viaf_compositions, outputfile):

    with codecs.open(outputfile, 'w') as csvfile:
        writer = csv.DictWriter(csvfile, delimiter=';', fieldnames=common.comprehensive_compositions_count_fieldnames, lineterminator='\n')
        writer.writeheader()

        compositions = neo4j_manager.load_compositions_from_csv(filename_viaf_compositions)
        reader = csv.DictReader(open(filename_authors), delimiter=';', fieldnames=common.wikidata_author_fieldnames, lineterminator='\n')
        firstTime = True
        for row in reader:
            if not firstTime:
                print 'row', row
                filtered_compositions = [composition for composition in compositions if composition[common.COMPOSITION_AUTHOR_ID_HEADER] == row[common.AUTHOR_VIAF_ID_HEADER]]
                author = row[common.AUTHOR_NAME_HEADER]
                gnd = row[common.GND_HEADER]
                viaf_len = len(filtered_compositions)
                freebase_id = row[common.FREEBASE_HEADER]
                freebase_id = freebase_id.replace(common.FREEBASE_PREFIX,'')
                print 'freebase id:', freebase_id
                freebase_len = 0
                if freebase_id:
                    name, freebase_len = freebase_helper.count_compositions(freebase_id)
                europeana_len = enrich.count_europeana_items(author)
                europeana_gnd_len = enrich.count_europeana_items_by_gnd(gnd)
                print 'gnd:', gnd, 'author:', author, 'VIAF len:', viaf_len, 'Freebase len:', freebase_len\
                    , 'Europeana references count by title:', europeana_len, 'Europeana references count by title:', europeana_gnd_len
                if author:
                    entry = build_comprehensive_composition_count_entry(gnd, author, viaf_len, freebase_len, europeana_len)
                    writer.writerow(entry)
            else:
                firstTime = False


# Command line parsing

if __name__ == '__main__':

    parser = argparse.ArgumentParser(
                    description="Mapping identifiers and data for dataset authors. Comprehensive queries over multiple repositories.")
    parser.add_argument('inputfile', type=str, nargs='+',
                    help="Input file to be processed")
    parser.add_argument('-o', '--outputfile', type=str, nargs='?',
                    default="data/comprehensive_compositions.csv",
                    help="Output file")


    if len(sys.argv) < 2:
        parser.print_help()
        sys.exit(1)

    args = parser.parse_args()
    retrieve_comprehensive_composition_count(args.inputfile, args.outputfile)
