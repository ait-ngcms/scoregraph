#!/usr/bin/env python
"""
Script for management of the neo4j database.

Invocation:
$ python neo4j_manager.py
"""

import argparse
import neo4jrestclient as client
from neo4jrestclient.client import GraphDatabase

NEO4J_DATABASE_URL = 'http://localhost:7474/db/data/'


# Main analyzing routine

def initialize():

    api_key = open("neo4j_api_key").read()
    gdb = GraphDatabase(NEO4J_DATABASE_URL, username='neo4j', password=api_key)

    # check node already exists in database
    #q = 'MATCH (u:User)-[r:likes]->(m:Beer) WHERE u.name="Marco" RETURN u, type(r), m'
    #results = gdb.query(q, returns=(client.Node, str, client.Node))
    #for r in results:
    #    print("(%s)-[%s]->(%s)" % (r[0]["name"], r[1], r[2]["name"]))

    # Create and specify properties for new node
    n = gdb.nodes.create(color="Red", width=16, height=32)
    n.set('key',1)
    print 'n', n

    value = n['key'] # Get property value
    print 'value', value
    value = 5
    n['key'] = value # Set property value
    value = n['key']
    print 'value', value


    # Create relationship
    n1 = gdb.nodes.create(color="Red", width=16, height=32)
    n2 = gdb.nodes.create(color="Red", width=16, height=32)
    n1.relationships.create("Knows", n2) # Useful when the name of
                                         # relationship is stored in a variable
    # Specify properties for new relationships
    n1.Knows(n2, since=123456789, introduced_at="Christmas party")
    print 'n1', n1

# Command line parsing

if __name__ == '__main__':

    parser = argparse.ArgumentParser(
                    description="Initialize neo4j database.")

    initialize()
