#!/usr/bin/env python
"""
Script for summarizing data for statistics generation.

Invocation:
$ python summarize.py data/enriched/*.json -o summary.csv
"""

import argparse
import csv
import json
#import simplejson as json
import os
import sys
import codecs


from common import write_json_file, progress, read_records


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

    #entries = []
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
#                for gnd in person['sameas']:
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
        #for notes in data['notes']:
        #    titles = [note for note in notes['notes']]

    print 'author: ', ' '.join(map(str,authors))
    print 'labels: ', ' '.join(map(str,labels))
    #titles_res = ' '.join(map(str,unicode(titles).encode('utf-8')))
    titles_res = unicode(titles).encode('utf-8')
    titles_res = titles_res.replace('[','').replace('u\'','').replace(']','').replace('\'','')
    print 'titles out: ', titles_res
    #print 'titles: ', ','.join(map(str,titles))

    entry = {
        'gnd': ' '.join(map(str,link_person_gnd)),
        'author': ' '.join(map(str,authors)),
        'subject': ' '.join(map(str,labels)),
        #'title': ' '.join(map(str,titles))
        #'title': ' '.join(map(str,unicode(titles).encode('utf-8')))
        ##'title': unicode(titles).encode('utf-8')
        'title': titles_res
    }

    #entries.append(entry)

#    if('related_europeana_items' in data):
#        for item in data['related_europeana_items']:
#            if('creator' in item):
#                authors = item['creator']
#                link_person_gnd = [link for link in authors
#                                        if 'gnd' in link]
#            if('title' in item):
#                titles = item['title']#

#            print 'author: ', ','.join(map(str,authors))
#            print 'titles: ', titles
#           print 'titles: ', ','.join(map(str,titles))
            #titles_res = unicode(titles).encode('utf-8')
            #titles_res = titles.replace('[u\'','').replace(']','')
            #print 'titles out: ', titles_res

###            entry = {
#                'gnd': unicode(',').encode('utf-8').join(map(str,link_person_gnd)),
#                'author': unicode(',').encode('utf-8').join(map(str,authors)),
##                'gnd': ','.join(map(str,link_person_gnd)),
###                'author': ','.join(map(str,authors)),
                #'title': unicode(',').encode('utf-8').join(map(str,titles))
##                'title': unicode(titles).encode('utf-8')
#                'title': ','.join(map(str,titles)).encode('utf-8')
                #'title': unicode(titles[0]).encode('utf-8')
#                'title': unicode(titles).encode('utf-8')
###                'title': titles
#                'title': titles_res
###            }

###            entries.append(entry)
    return entry

def summarize_titles(inputfiles, outputfile):
    print("Summarizing", len(inputfiles), "titles in", outputfile)
    with codecs.open(outputfile, 'w', 'utf-8') as csvfile:
        fieldnames = ['gnd',
                      'author',
                      'subject',
                      'title']
        writer = csv.DictWriter(csvfile, delimiter=';', fieldnames=fieldnames)
        writer.writeheader()
        for filename, record in read_records(inputfiles):
            #record = unicode(record, 'iso-8859-15')
            data = json.loads(record, encoding='utf-8')
#            entries = summarize_titles_data(data)
            entry = summarize_titles_data(data)
#            [writer.writerow(entry) for entry in entries]
            writer.writerow(entry)

def read_summary(inputfile):
    print 'Read summary: ', inputfile
    with open(inputfile, 'rb') as csvfile:
        summary_reader = csv.reader(csvfile, delimiter=';', quotechar='|')
        summary = []
        for row in summary_reader:
            #print ', '.join(row)
            summary.append(', '.join(row[1:]))
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
