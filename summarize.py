#!/usr/bin/env python
"""
Script for summarizing data for statistics generation.

Invocation:
$ python summarize.py data/enriched/*.json -o summary.csv
"""

import argparse
import csv
import json
import sys
import codecs

import goslate

from common import read_records

import dbpedia_helper


DOC_ID_JSON = 'doc_id'
PERSONS_JSON = 'persons'
SAMEAS_JSON = 'sameas'
GND_JSON = 'gnd'
NAME_JSON = 'name'

gs = goslate.Goslate()


def summarize(data):
    
    artwork_links = 0
    if('sameas' in data):
        artwork_links = len(data['sameas'])

    persons = 0
    if('persons' in data):
        persons = len([person for person in data['persons']])

    links_person_gnd = 0
    links_person_dbpedia = 0
    links_person_viaf = 0
    if('persons' in data):
        for person in data['persons']:
            if('sameas' in person):
                links = [link for link in person['sameas']]
                links_person_gnd += len([link for link in links
                                        if 'gnd' in link])
                links_person_dbpedia += len([link for link in links
                                        if 'dbpedia' in link])
                links_person_viaf += len([link for link in links
                                        if 'viaf' in link])

    related_europeana_items = 0
    if('related_europeana_items' in data):
        related_europeana_items = len(data['related_europeana_items'])

    entry = {
        'id': data['aleph_id'],
        'links_artwork': artwork_links,
        'persons': persons,
        'links_person_gnd': links_person_gnd,
        'links_person_dbpedia': links_person_dbpedia,
        'links_person_viaf': links_person_viaf,
        'related_europeana_items': related_europeana_items
    }

    return entry


def summarize_titles_data(data):

    global link_person_gnd
    link_person_gnd = []
    global titles
    titles = []
    global authors
    authors = []
    global labels
    labels = []
    if('persons' in data):
        for person in data['persons']:
            authors.append(person['name'])
            if('sameas' in person):
                 links = [link for link in person['sameas']]
                 link_person_gnd_tmp = [link for link in links
                                         if 'gnd' in link]
                 link_person_gnd.append(' '.join(link_person_gnd_tmp))


    if('terms' in data):
        for subject in data['terms']:
            if('labels' in subject):
                labels = [label for label in subject['labels']]

    if('notes' in data):
        titles = data['notes']
        print data['notes']

    print 'author: ', ' '.join(map(str,authors))
    print 'labels: ', ' '.join(map(str,labels))

    titles_res = ''
    str_list = []
    for elem in titles:
        byte_str = elem.encode('utf8', 'ignore')
        titles_res = titles_res + byte_str + ' '
        str_list.append(byte_str)
    print 'titles out: ', titles_res

    print 'en translation: ', gs.translate(titles_res, 'en')

    entry = {
        'gnd': ' '.join(map(str,link_person_gnd)),
        'author': ' '.join(map(str,authors)),
        'subject': ' '.join(map(str,labels)),
        'title': titles_res
    }

    return entry


def summarize_authors_data(data, fieldnames):

    entries = []
    onb_id = ''

    if(DOC_ID_JSON in data):
        onb_id = data[DOC_ID_JSON]
        print data[DOC_ID_JSON]

    if(PERSONS_JSON in data):
        isFirstTime = True
        for person in data[PERSONS_JSON]:
            author = person[NAME_JSON]
            link_person_gnd = []
            if(SAMEAS_JSON in person):
                 links = [link for link in person[SAMEAS_JSON]]
                 link_person_gnd = [link for link in links
                                         if GND_JSON in link]

            dbpedia_items = dbpedia_helper.find_dbpedia_items(author)
            dbpedia_id_res = ''
            for key, value in dbpedia_items.iteritems():
                dbpedia_id = dbpedia_helper.find_dbpedia_id(key)
                byte_str = dbpedia_id.encode('utf8', 'ignore')
                print 'DBPedia ID', byte_str
                dbpedia_id_res = byte_str + ' ' + dbpedia_id_res

            if(isFirstTime == False):
                onb_id = ''

            values = [onb_id, author, link_person_gnd[0], dbpedia_id_res]
            entry = dict(zip(fieldnames, values))
            entries.append(entry)
            isFirstTime = False

    return entries


def summarize_titles(inputfiles, outputfile):

    print("Summarizing", len(inputfiles), "titles in", outputfile)
    with codecs.open(outputfile, 'w') as csvfile:
        fieldnames = ['gnd',
                      'author',
                      'subject',
                      'title']

        writer = csv.DictWriter(csvfile, delimiter=';', fieldnames=fieldnames)
        writer.writeheader()
        for filename, record in read_records(inputfiles):
            data = json.loads(record, encoding='utf-8')
            entry = summarize_titles_data(data)
            writer.writerow(entry)


def summarize_authors(inputfiles, outputfile):

    print("Summarizing", len(inputfiles), "authors in", outputfile)
    with codecs.open(outputfile, 'w') as csvfile:
        fieldnames = ['onb id',
                      'author name',
                      'gnd url',
                      'dbpedia id']

        writer = csv.DictWriter(csvfile, delimiter=';', fieldnames=fieldnames, lineterminator='\n')
        writer.writeheader()
        for filename, record in read_records(inputfiles):
            data = json.loads(record, encoding='utf-8')
            entries = summarize_authors_data(data, fieldnames)
            writer.writerows(entries)


def read_summary(inputfile):
    print 'Read summary: ', inputfile
    with open(inputfile, 'rb') as csvfile:
        summary_reader = csv.reader(csvfile, delimiter=';', quotechar='|')
        summary = []
        for row in summary_reader:
            row_str = unicode(', '.join(row[1:]), 'utf-8')
            summary.append(row_str)
        return summary


def read_csv_summary(inputfile):
    print 'Read summary: ', inputfile
    with open(inputfile, 'rb') as csvfile:
        summary_reader = csv.reader(csvfile, delimiter=';', quotechar='|')
        summary = []
        for row in summary_reader:
            summary.append(row)
        return summary


# Main summarization routine

def summarize_records(inputfiles, outputfile):
    print("Summarizing", len(inputfiles), "records in", outputfile)
    with open(outputfile, 'w') as csvfile:
        fieldnames = ['id',
                      'links_artwork',
                      'persons',
                      'links_person_gnd',
                      'links_person_dbpedia',
                      'links_person_viaf',
                      'related_europeana_items']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for filename, record in read_records(inputfiles):
            data = json.loads(record)
            entry = summarize(data)
            writer.writerow(entry)


# Command line parsing

if __name__ == '__main__':

    parser = argparse.ArgumentParser(
                    description="Collecting data for dataset statistics.")
    parser.add_argument('inputfiles', type=str, nargs='+',
                    help="Input files to be processed")
    parser.add_argument('-o', '--outputfile', type=str, nargs='?',
                    default="data/summary.csv",
                    help="Output file")


    if len(sys.argv) < 2:
        parser.print_help()
        sys.exit(1)

    args = parser.parse_args()
    summarize_records(args.inputfiles, args.outputfile)
