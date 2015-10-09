#!/usr/bin/env python
"""
Script for summarizing data for statistics generation.

Invocation:
$ python viaf_helper.py data/mapped_authors.csv -o data/viaf_compositions.csv
"""

import argparse
import csv
import sys
import codecs

import common

import summarize

import xml.etree.ElementTree as ET


VIAF_API_URL = 'http://www.viaf.org/viaf/'


viaf_compositions_fieldnames = [
    'author id'
    , 'work id'
    , 'title'
]


# e.g. http://www.viaf.org/viaf/61732497/viaf.xml
def retrieve_viaf_compositions_by_author_id(viaf_id, outputfile):

    query_author = VIAF_API_URL + str(viaf_id) + '/viaf.xml'
    print 'query author:', query_author
    author_response = common.process_http_query(query_author)
    print 'viaf author:', author_response
    if author_response.content:
        root = ET.fromstring(author_response.content)
        parse_response(viaf_id, root, outputfile)
    return author_response


def parse_response(viaf_id, root, outputfile):

    for child in root:
        #print child
        if 'titles' in child.tag:
            for work in child:
                #print work
                for elem in work:
                    if 'title' in elem.tag:
                        print 'viaf author id:', viaf_id, 'composition id:', work.attrib.get('id'), \
                            'title: ', elem.text
                        entry = build_viaf_composition_entry(viaf_id, work.attrib.get('id'), elem.text)
                        write_composition_in_csv_file(outputfile, entry)


def build_viaf_composition_entry(
        author_id, composition_id, title):

    values = [
        author_id
        , composition_id
        , common.toByteStr(title)
    ]

    return dict(zip(viaf_compositions_fieldnames, values))


def write_composition_in_csv_file(outputfile, entry):

    with open(outputfile, 'ab') as csvfile:
        writer = csv.DictWriter(csvfile, delimiter=';', fieldnames=viaf_compositions_fieldnames, lineterminator='\n')
        writer.writerow(entry)


# Main mapping routine

def retrieve_authors_data_by_viaf_id(inputfile, outputfile):

    with codecs.open(outputfile, 'w') as csvfile:
        writer = csv.DictWriter(csvfile, delimiter=';', fieldnames=viaf_compositions_fieldnames, lineterminator='\n')
        writer.writeheader()

    summary = summarize.read_csv_summary(inputfile)
    for row in summary[1:]: # ignore first row, which is a header
        VIAF_ID_COL = 7
        viaf_id = row[VIAF_ID_COL]
        print 'viaf ID:', viaf_id
        for id in viaf_id.split(common.BLANK):
            retrieve_viaf_compositions_by_author_id(id, outputfile)


# Command line parsing

if __name__ == '__main__':

    parser = argparse.ArgumentParser(
                    description="Mapping identifiers and data for dataset authors. VIAF query.")
    parser.add_argument('inputfile', type=str, nargs='+',
                    help="Input file to be processed")
    parser.add_argument('-o', '--outputfile', type=str, nargs='?',
                    default="data/viaf_compositions.csv",
                    help="Output file")


    if len(sys.argv) < 2:
        parser.print_help()
        sys.exit(1)

    args = parser.parse_args()
    retrieve_authors_data_by_viaf_id(args.inputfile, args.outputfile)
