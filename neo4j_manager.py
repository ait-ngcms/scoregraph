#!/usr/bin/env python
"""
Script for management of the neo4j database.

Invocation:
$ python neo4j_manager.py
"""

import common
import argparse
import csv
from neo4jrestclient import client
from neo4jrestclient.client import GraphDatabase
# directory structure
from os import walk

NEO4J_DATABASE_URL = 'http://localhost:7474/db/data/'
NEO4J_API_KEY_FILE_NAME = "neo4j_api_key"
USER_NAME = 'neo4j'
NAME = 'name'

AUTHOR_LABEL = 'Author'
COMPOSITION_LABEL = 'Composition'
JSON_WIKIDATA_AUTHOR_DATA_LABEL = 'Json Wikidata Author Data'
JSON_CONTENT = 'JSON Content'


class Neo4jManager:

    def __init__(self):
        self.gdb = initialize()


    #def label_exists(self, label_name):#

    #    res = False
    #    q = 'MATCH (a:' + label_name + ') RETURN a;'
    #    results = self.gdb.query(q)
    #    if results:
    #        res = True
    #    return res


    def node_exists(self, label, author):

        res = False
        q = 'MATCH (a:' + label + ') WHERE a.name = "' + author + '" RETURN a;'
        results = self.gdb.query(q)
        if results:
            res = True
        return res


    def create_label(self, label_name):

        #res = self.gdb.labels.get(label_name)
        #if res == None:
        res = self.gdb.labels.create(label_name)
        return res


    def create_author(self, db_label, author):

        #author_node = self.gdb.nodes.get(author[NAME])
        #if not author_node:
        author_node = self.gdb.nodes.create(name=author[NAME])
#        for field, index in enumerate(common.wikidata_author_fieldnames):
        for field in author:
            #author_node.set(field.key, field.value)
            author_node.set(field, author[field])
        db_label.add(author_node)
        return author_node


    def create_json_entry(self, db_label, content, name):

        json_node = self.gdb.nodes.create(name=name)
        content_str = common.convert_json_dict_to_string(content)
        json_node.set(JSON_CONTENT, content_str)
        db_label.add(json_node)
        return json_node


    def create_author_with_label(self, author):

        author_label = self.create_label(AUTHOR_LABEL)
        return self.create_author(author_label, author)



    def create_composition(self, db_label, composition):

        #composition_node = self.gdb.nodes.get(composition)
        #if not composition_node:
        composition_node = self.gdb.nodes.create(name=composition)
        db_label.add(composition_node)
        return composition_node


    def create_composition_with_label(self, composition):

        composition_label = self.create_label(COMPOSITION_LABEL)
        return self.create_composition(composition_label, composition)


    def create_relationship(self, node1, node2, name):

        node1.relationships.create(name, node2)


    def query_composition_by_author_name(self, author):

        q = 'MATCH (a:Author)-[r:has_composition]->(m:Composition) WHERE a.name="' \
            + author + '" RETURN a, type(r), m'
        results = self.gdb.query(q, returns=(client.Node, str, client.Node))
        if results:
            for r in results:
                print("(%s)-[%s]->(%s)" % (r[0][NAME], r[1], r[2][NAME]))
            return results[0][2][NAME]
        return None


    def query_json(self, query):

        q = "MATCH (a:`" + JSON_WIKIDATA_AUTHOR_DATA_LABEL + "`) WHERE a.name='" \
            + query + "' RETURN a"
        results = self.gdb.query(q, returns=(client.Node, str, client.Node))
        if results:
            for r in results:
                node = r[0]
                content = node.properties[JSON_CONTENT]
                print("(%s)" % (content))
                return content
        return None


    def remove_all_nodes(self):

        q = 'MATCH (n) OPTIONAL MATCH (n)-[r]-() DELETE n,r'
        self.gdb.query(q)
        print 'All nodes removed in Neo4J database.'


    def save_author_with_compositions(self, row_author, compositions, author_label, composition_label):

        author_node = self.create_author(author_label, row_author)
        #reader = csv.DictReader(open(filename_compositions), delimiter=';', fieldnames=common.viaf_compositions_fieldnames, lineterminator='\n')
        for row_composition in compositions:
            if row_composition[common.COMPOSITION_AUTHOR_ID_HEADER] == row_author[common.AUTHOR_VIAF_ID_HEADER]:
                composition_node = self.create_composition(composition_label, row_composition[common.COMPOSITION_TITLE_HEADER])
                self.create_relationship(author_node, composition_node, common.RELATION_AUTHOR_TO_COMPOSITION)
        composition_name = self.query_composition_by_author_name(author_node[NAME])
#        print 'found composition', composition_name, 'for author', author_node[NAME]


    def save_json_wikidata_author_data(self, author_label, data, name):

        author_node = self.create_json_entry(author_label, data, name)


def load_compositions_from_csv(filename_compositions):

    compositions = []
    reader = csv.DictReader(open(filename_compositions), delimiter=';', fieldnames=common.viaf_compositions_fieldnames, lineterminator='\n')
    for row_composition in reader:
        compositions.append(row_composition)
    return compositions


def save_mapped_authors_from_csv(filename_authors, filename_compositions):

    neo_db = Neo4jManager()
    neo_db.remove_all_nodes()
    author_label = neo_db.create_label(AUTHOR_LABEL)
    composition_label = neo_db.create_label(COMPOSITION_LABEL)

    compositions = load_compositions_from_csv(filename_compositions)

    reader = csv.DictReader(open(filename_authors), delimiter=';', fieldnames=common.wikidata_author_fieldnames, lineterminator='\n')
    firstTime = True
    for row in reader:
        if not firstTime:
            print 'row', row
            filtered_compositions = [composition for composition in compositions if composition[common.COMPOSITION_AUTHOR_ID_HEADER] == row[common.AUTHOR_VIAF_ID_HEADER]]
            print 'len compositions', len(filtered_compositions)
            neo_db.save_author_with_compositions(row, filtered_compositions, author_label, composition_label)
        else:
            firstTime = False


def save_json_wikidata_author_data_dir(inputdir):

    neo_db = Neo4jManager()
    json_wikidata_author_data_label = neo_db.create_label(JSON_WIKIDATA_AUTHOR_DATA_LABEL)
    for (dirpath, dirnames, filenames) in walk(inputdir):
        for filename in filenames:
            data = common.read_json_file(dirpath + common.SLASH + filename)
            neo_db.save_json_wikidata_author_data(json_wikidata_author_data_label, data, filename.replace(common.JSON_EXT,''))


def search_in_json_neo4j(query):

    neo_db = Neo4jManager()
    neo_db.query_json(query)


# Main analyzing routine

def initialize():

    api_key = open(NEO4J_API_KEY_FILE_NAME).read()
    gdb = GraphDatabase(NEO4J_DATABASE_URL, username=USER_NAME, password=api_key)
    return gdb


# Command line parsing

if __name__ == '__main__':

    parser = argparse.ArgumentParser(
                    description="Initialize neo4j database.")

    initialize()
