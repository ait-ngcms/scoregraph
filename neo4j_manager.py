#!/usr/bin/env python
"""
Script for management of the neo4j database.

Invocation:
$ python neo4j_manager.py
"""

import common
import argparse
from neo4jrestclient import client
from neo4jrestclient.client import GraphDatabase

NEO4J_DATABASE_URL = 'http://localhost:7474/db/data/'
NEO4J_API_KEY_FILE_NAME = "neo4j_api_key"
USER_NAME = 'neo4j'
NAME = 'name'

AUTHOR_LABEL = 'Author'
COMPOSITION_LABEL = 'Composition'


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

        res = self.gdb.labels.get(label_name)
        if res == None:
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


    def create_composition(self, db_label, composition):

        #composition_node = self.gdb.nodes.get(composition)
        #if not composition_node:
        composition_node = self.gdb.nodes.create(name=composition)
        db_label.add(composition_node)
        return composition_node


    def create_relationship(self, node1, node2, name):

        node1.relationships.create(name, node2)


    def query_composition_by_author_name(self, author):

        q = 'MATCH (a:Author)-[r:has_composition]->(m:Composition) WHERE a.name="' \
            + author + '" RETURN a, type(r), m'
        results = self.gdb.query(q, returns=(client.Node, str, client.Node))
        for r in results:
            print("(%s)-[%s]->(%s)" % (r[0][NAME], r[1], r[2][NAME]))
        return results[0][2][NAME]



# Main analyzing routine

def initialize():

    api_key = open(NEO4J_API_KEY_FILE_NAME).read()
    gdb = GraphDatabase(NEO4J_DATABASE_URL, username=USER_NAME, password=api_key)

    # check node already exists in database
#    q = 'MATCH (a:Author)-[r:has_composition]->(m:Composition) WHERE a.name="Mahler, Gustav" RETURN a, type(r), m'
#    results = gdb.query(q, returns=(client.Node, str, client.Node))
#    for r in results:
#        print("(%s)-[%s]->(%s)" % (r[0]["name"], r[1], r[2]["name"]))

    # Create author nodes with labels
#    author = gdb.labels.create("Author")
#    a1 = gdb.nodes.create(name="Mahler, Gustav")
#    a1.set('wikidata', 7304)
#    author.add(a1)
#    a2 = gdb.nodes.create(name="Reitler, Josef")
#    a2.set('wikidata', 1705542)
#    author.add(a2)
#    print 'author', author

    # Create composition nodes with labels
#    composition = gdb.labels.create("Composition")
#    c1 = gdb.nodes.create(name="das klagende lied")
#    composition.add(c1)

    # Create relationship
#    a1.relationships.create("has_composition", c1)

    # Create and specify properties for new node
    #n = gdb.nodes.create(color="Red", width=16, height=32)
    #n.set('key',1)
    #print 'n', n

    #value = n['key'] # Get property value
    #print 'value', value
    #value = 5
    #n['key'] = value # Set property value
    #value = n['key']
    #print 'value', value
    return gdb


    # Create relationship
#    n1 = gdb.nodes.create(color="Red", width=16, height=32)
#    n2 = gdb.nodes.create(color="Red", width=16, height=32)
#    n1.relationships.create("Knows", n2) # Useful when the name of
                                         # relationship is stored in a variable
    # Specify properties for new relationships
#    n1.Knows(n2, since=123456789, introduced_at="Christmas party")
#    print 'n1', n1

# Command line parsing

if __name__ == '__main__':

    parser = argparse.ArgumentParser(
                    description="Initialize neo4j database.")

    initialize()
