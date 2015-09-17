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

import common

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


def summarize_titles_data(data, fieldnames):

    global link_person_gnd
    link_person_gnd = []
    global titles
    titles = []
    global authors
    authors = []
    global labels
    labels = []
    if(PERSONS_JSON in data):
        for person in data[PERSONS_JSON]:
            name = common.toByteStr(person[NAME_JSON])
            authors.append(name)
            if(SAMEAS_JSON in person):
                 links = [link for link in person[SAMEAS_JSON]]
                 link_person_gnd_tmp = [link for link in links
                                         if GND_JSON in link]
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
        byte_str = common.toByteStr(elem)
        titles_res = titles_res + byte_str + ' '
        str_list.append(byte_str)
    print 'titles out: ', titles_res

    print 'en translation: ', gs.translate(titles_res, 'en')

    #entry = {
    #    'gnd': ' '.join(map(str,link_person_gnd)),
    #    'author': ' '.join(map(str,authors)),
    #    'subject': ' '.join(map(str,labels)),
    #    'title': titles_res
    #}

    values = [
        ' '.join(map(str,link_person_gnd))
        , ' '.join(map(str,authors))
        , ' '.join(map(str,labels))
        , titles_res
    ]

    entry = dict(zip(fieldnames, values))
    return entry


def summarize_authors_data(data, fieldnames):

    entries = []
    onb_id = ''

    if(DOC_ID_JSON in data):
        onb_id = data[DOC_ID_JSON]
        print data[DOC_ID_JSON]

    if(PERSONS_JSON in data):
        #isFirstTime = True
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
                dbpeida_id_str = common.toByteStr(dbpedia_id)
                print 'DBPedia ID', dbpeida_id_str
                dbpedia_id_res = dbpeida_id_str + ' ' + dbpedia_id_res

            #if(isFirstTime == False):
            #    onb_id = ''
            gnd = ''
            if(len(link_person_gnd) > 0):
                gnd = link_person_gnd[0]

            values = [
                onb_id
                , common.toByteStr(author)
                , gnd
                , dbpedia_id_res
            ]
            entry = dict(zip(fieldnames, values))
            entries.append(entry)
            #isFirstTime = False

    return entries


def summarize_titles(inputfiles, outputfile):

    print("Summarizing", len(inputfiles), "titles in", outputfile)
    with codecs.open(outputfile, 'w') as csvfile:
        fieldnames = ['gnd',
                      'author',
                      'subject',
                      'title']

        writer = csv.DictWriter(csvfile, delimiter=';', fieldnames=fieldnames, lineterminator='\n')
        writer.writeheader()
        for filename, record in read_records(inputfiles):
            data = json.loads(record, encoding='utf-8')
            entry = summarize_titles_data(data, fieldnames)
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


def correct_authors(outputfile):

    print("Correct authors in", outputfile)
    with open(outputfile.replace('authors','authors-new'), "wt") as fout:
        f = codecs.open(outputfile, 'r')
        prev_id = ''
        for idx, line in enumerate(f):
            print repr(line)
            cur_id = line.split(";")[0]
            if cur_id:
                prev_id = cur_id
            print 'cur_id', cur_id, 'prev_id', prev_id
            if idx > 1 and (not cur_id or cur_id == ''):
                fout.write(prev_id + line)
            else:
                # insert previous ID
                fout.write(line)
            #if idx == 5:
            #    break


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
