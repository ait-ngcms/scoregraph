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
import wikidata_helper
import enrich # connection to Europeana

import xml.etree.ElementTree as ET


def build_comprehensive_composition_count_entry(
        gnd, author_name, viaf_len, freebase_len, europeana_len, europeana_gnd_len, europeana_gnd_sameas_len, europeana_all_len):

    values = [
        gnd
        , author_name
        , viaf_len
        , freebase_len
        , europeana_len
        , europeana_gnd_len
        , europeana_gnd_sameas_len
        , europeana_all_len
    ]

    return dict(zip(common.comprehensive_compositions_count_fieldnames, values))


def retrieve_sameas_urls(filename_sameas):

    reader = csv.DictReader(open(filename_sameas), delimiter=';', fieldnames=common.sameas_fieldnames, lineterminator='\n')
    res = {}
    firstTime = True
    for row in reader:
        if not firstTime:
            print 'row', row
            res[row['gnd url']] = row['sameAs']
        else:
            firstTime = False
    return res


def map_composition_data_in_csv(filename_mapped_authors, outputfile):

    wikidata_helper.retrieve_wikidata_compositions_by_musicbrainz_id(filename_mapped_authors, outputfile)

# Main mapping routine

def retrieve_comprehensive_composition_count(filename_authors, filename_viaf_compositions, filename_sameas, outputfile):

    with codecs.open(outputfile, 'w') as csvfile:
        writer = csv.DictWriter(csvfile, delimiter=';', fieldnames=common.comprehensive_compositions_count_fieldnames, lineterminator='\n')
        writer.writeheader()

        compositions = neo4j_manager.load_compositions_from_csv(filename_viaf_compositions)
        sameas = retrieve_sameas_urls(filename_sameas)
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
                #europeana_len = enrich.count_europeana_items(author)
                query = author
                query_all = author
                if ',' in author:
                    params = author.split(',')
                    query = 'who:(' + "\"" + params[0] + ' ' + params[1] + "\"" + ' OR ' + "\"" + params[1] + ' ' + params[0] + "\"" + ')'
                    query_all = '(' + params[0] + ' ' + params[1] + ') OR (' + params[1] + ' ' + params[0] + ')'
                europeana_len = enrich.count_europeana_items(query)
                europeana_all_len = enrich.count_europeana_items(query_all)
                europeana_gnd_len = enrich.count_europeana_items("edm_agent:\"" + gnd + "\"")
                sameas_urls = sameas[gnd].replace(" ", "\" OR \"")
                #sameas_or_str = "\"" + " OR " + "\""
#                europeana_gnd_sameas_len = enrich.count_europeana_items("edm_agent:(\"" + gnd + sameas_or_str.join(map(str,sameas_urls)) + ")")
                europeana_gnd_sameas_len = enrich.count_europeana_items("edm_agent:(\"" + sameas_urls + "\")")
                print 'gnd:', gnd, 'author:', author, 'VIAF len:', viaf_len, 'Freebase len:', freebase_len\
                    , 'Europeana references count by title:', europeana_len, 'Europeana references count by GND:', europeana_gnd_len\
                    , 'Europeana references count by GND and sameAs:', europeana_gnd_sameas_len\
                    , 'Europeana references count by title for all variations', europeana_all_len

                if author:
                    entry = build_comprehensive_composition_count_entry(
                        gnd, author, viaf_len, freebase_len, europeana_len, europeana_gnd_len, europeana_gnd_sameas_len, europeana_all_len)
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
