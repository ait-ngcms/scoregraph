#!/usr/bin/env python
"""
Script for extracting relevant data points from raw data.

Invocation:
$ python normalize.py data/raw/*.xml -o data/normalized
"""

import argparse
import sys
import os

from bs4 import BeautifulSoup

from common import write_json_file, progress, read_records

GND_PREFIX = "http://d-nb.info/gnd"


# Metadata extraction functions

def find_tags_in_id_range(soup, start, end):
    tags = [tag for tag in soup.find_all("varfield")
            if tag['id'] and tag['id'].isnumeric() and
            int(tag['id']) >= start and int(tag['id']) < end]
    return tags


def persons(soup):
    persons = []
    tags = find_tags_in_id_range(soup, 100, 200)
    for tag in tags:
        person = {}
        # name
        name = tag.find(label="p")
        if not name:
            name = tag.find(label="a")
        ext = ''
        if ' ' not in name.string:
            ext_tag = tag.find(label="n")
            if ext_tag:
                ext = ' ' + tag.find(label="n").string
        if name:
            person['name'] = name.string + ext
        # lifetime
        lifetime = tag.find(label="d")
        if lifetime:
            person['lifetime'] = lifetime.string
        # role
        role = tag.find(label="b")
        if role:
            role = role.string.replace("[", "").replace("]", "")
            person['role'] = role
        # gnd_link
        gnd_link = tag.find(label="9")
        if gnd_link:
            gnd_link = GND_PREFIX + "/" + gnd_link.string[8:]
            person['sameas'] = [gnd_link]
        persons.append(person)
    return persons


def content(soup):
    contents = []
    for tag in soup.find_all("varfield", id="655"):
        uri = tag.find("subfield", label="u")
        note = tag.find("subfield", label="z")
        if note:
            note = note.string
        if uri:
            content = {
                'uri': uri.string,
                'note': note
            }
            contents.append(content)
    return contents


def title(soup):
    tag = soup.find("varfield", id="303")
    if not tag:
        return None
    else:
        title = tag.find("subfield", label="t")
        if not title:
            return None
        else:
            return title.string


def subtitles(soup):
    subtitles = []
    tags = find_tags_in_id_range(soup, 304, 400)
    for tag in tags:
        subfields = tag.find_all("subfield")
        for subfield in subfields:
            contents = subfield.string.replace("[", "").replace("]", "")
            subtitles.append(contents)
    return subtitles


def dates(soup):
    dates = []
    date_tags = soup.find_all("varfield", id="425")
    for tag in date_tags:
        dates.append(tag.subfield.string)
    return dates


def gnd_link(soup):
    tag = soup.find("varfield", id="303")
    if not tag:
        return None
    else:
        gnd_link = tag.find("subfield", label="9")
        if not gnd_link:
            return None
        else:
            return GND_PREFIX + "/" + gnd_link.string[8:]


def notes(soup):
    notes = []
    tags = find_tags_in_id_range(soup, 400, 600)
    for tag in tags:
        if tag.id != "425":
            for subfield in tag.find_all("subfield", label="a"):
                notes.append(subfield.string)
    return notes


def terms(soup):
    terms = []
    tags = find_tags_in_id_range(soup, 900, 1000)
    for tag in tags:
        term = {}
        labels = []
        for subfield in tag.find_all("subfield"):
            if subfield["label"] != "9":
                labels.append(subfield.string)
        if(len(labels) > 0):
            term['labels'] = labels
        gnd_link = tag.find("subfield", label="9")
        if gnd_link:
            gnd_link = GND_PREFIX + "/" + gnd_link.string[8:]
            term['sameas'] = [gnd_link]
        terms.append(term)
    return terms


def doc_id(soup):
    return soup.doc_number.string


def aleph_id(soup):
    return soup.find("varfield", id="001").find("subfield", label="a").string


# Main normalization routine

def normalize(raw_record):
    soup = BeautifulSoup(raw_record, "html.parser")
    normalized_record = {}

    # mandatory fields
    normalized_record['aleph_id'] = aleph_id(soup)
    normalized_record['doc_id'] = doc_id(soup)

    # optional fields
    if title(soup):
        normalized_record['title'] = title(soup)
    if subtitles(soup):
        normalized_record['subtitles'] = subtitles(soup)
    if persons(soup):
        normalized_record['persons'] = persons(soup)
    if content(soup):
        normalized_record['content'] = content(soup)
    if dates(soup):
        normalized_record['dates'] = dates(soup)
    if notes(soup):
        #normalized_notes = notes(soup)
        #print 'normalized_notes: ', normalized_notes
        #new_notes = [unicode(elem).encode('utf-8') for elem in normalized_notes]
        #normalized_record['notes'] = new_notes
        #print 'new_notes: ', new_notes
#        normalized_record['notes'] = notes(soup)
        normalized_record['notes'] = notes(soup)
    if terms(soup):
        normalized_record['terms'] = terms(soup)
    if gnd_link(soup):
        normalized_record['sameas'] = [gnd_link(soup)]

    return normalized_record


def normalize_records(inputfiles, outputdir):
    print("Normalizing", len(inputfiles), "records to", outputdir)
    for index, (filename, record) in enumerate(read_records(inputfiles)):
        progress((index+1)/len(inputfiles))
        normalized_record = normalize(record)
        print 'normalized record: ', normalized_record
        #new_normalized_record = [unicode(elem).encode('utf-8') for elem in normalized_record]
        #print 'new normalized record: ', new_normalized_record

        out_file = os.path.basename(filename).replace("xml", "json")
        write_json_file(outputdir, out_file, normalized_record)
#        write_json_file(outputdir, out_file, new_normalized_record)


# Command line parsing

if __name__ == '__main__':

    parser = argparse.ArgumentParser(
                    description="Extract relevant fields and convert to JSON.")
    parser.add_argument('inputfiles', type=str, nargs='+',
                    help="Input files to be processed")
    parser.add_argument('-o', '--outputdir', type=str, nargs='?',
                    default="data/normalized",
                    help="Output directory")

    if len(sys.argv) < 2:
        parser.print_help()
        sys.exit(1)

    args = parser.parse_args()
    normalize_records(args.inputfiles, args.outputdir)
