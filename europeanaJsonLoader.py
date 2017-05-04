##
# This module performs loading of Europeana JSON files by URLs.
##

import json
import urllib
import json
import codecs
import os


INPUT_FILE = 'agents_in_europeana.txt'
JSON_EXT = '.json'
OUTPUT_DIR = 'output'
SLASH = '/'


def ensure_directory(outputdir):

    """Make sure that output directory exists"""
    if not os.path.exists(outputdir):
        print("Creating directory", outputdir)
        os.makedirs(outputdir)


def write_json_file(outputdir, filename, data):

    ensure_directory(outputdir)
    with codecs.open(outputdir + SLASH + filename, "w", 'utf-8') as out_file:
        json.dump(data, out_file, sort_keys=True, indent=4, ensure_ascii=False, encoding='utf-8')


def unitTest():

    print('Loading of Europeana agents started ...')

    agents = []

    # read agents links from input file
    with open(INPUT_FILE) as f:
        agent = f.readlines()
        # remove whitespace characters
        agents = [x.strip() for x in agent]

    for agent in agents:

        try:
            url_address = agent.replace("http://data.europeana.eu/agent/base/", "http://test-entity.europeana.eu/entity/agent/base/") + '?wskey=apidemo'
            response = json.loads(urllib.urlopen(url_address).read())
            author_id = agent.split(SLASH)[-1]
            print('Loading agent url:' + agent)
            filename = str(author_id) + JSON_EXT
            write_json_file(OUTPUT_DIR, filename, response)
        except Exception as ex:
            print(ex)

    assert(len(agents) > 0)
    print('Loading of Europeana agents completed.')


unitTest()
    
