##
# This module performs loading of Europeana JSON files by URLs.
##

import json
import urllib
import json
import codecs
import os
import glob


INPUT_FILE = 'agents_in_europeana.txt'
JSON_EXT = '.json'
OUTPUT_DIR = 'output'
OUTPUT_DIR_2 = 'output2'
SLASH = '/'


def is_stored_as_json_file(path):

    response_json = False
    inputfile = glob.glob(path)
    if(inputfile):
        #print 'exists:', inputfile
        response_json = True #read_json_file(inputfile[0])

    return response_json


def ensure_directory(outputdir):

    """Make sure that output directory exists"""
    if not os.path.exists(outputdir):
        print("Creating directory", outputdir)
        os.makedirs(outputdir)


def write_json_file(outputdir, filename, data):

    ensure_directory(outputdir)
    with codecs.open(outputdir + SLASH + filename, "w", 'utf-8') as out_file:
        json.dump(data, out_file, sort_keys=True, indent=4, ensure_ascii=False, encoding='utf-8')


def save_json(outputdir, filename, data):

    res = True

    try:
        response = json.loads(data)
        # print('Loading agent url:', agent, ', count:', count)
        write_json_file(outputdir, filename, response)

    except Exception as ex:
        print('Error loading JSON file:', filename, 'content:', data)
        print(ex)
        res = False

    return res


def save_text(outputdir, filename, data):

    output = open(outputdir + SLASH + filename,'a')
    output.write(str(data))
    output.close()


def unitTest():

    print('Loading of Europeana agents started ...')

    agents = []

    # read agents links from input file
    with open(INPUT_FILE) as f:
        agent = f.readlines()
        # remove whitespace characters
        agents = [x.strip() for x in agent]

    count = 0
    for agent in agents:

        author_id = None
        url_address = None
        try:
            url_address = agent.replace("http://data.europeana.eu/agent/base/", "http://test-entity.europeana.eu/entity/agent/base/") + '?wskey=apidemo'
            author_id = agent.split(SLASH)[-1]
            filename = str(author_id) + JSON_EXT

            if not is_stored_as_json_file(OUTPUT_DIR + SLASH + filename):
                content = urllib.urlopen(url_address).read()
                if not save_json(OUTPUT_DIR_2, filename, content):
                    save_text(OUTPUT_DIR_2, filename, content)
                #response = json.loads(content)
                #print('Loading agent url:', agent, ', count:', count)
                #write_json_file(OUTPUT_DIR_2, filename, response)

        except Exception as ex:
            print('Error loading agent:', agent, 'id:', author_id, 'url:', url_address, 'count:', count)
            print(ex)

        count = count + 1

    assert(len(agents) > 0)
    print('Loading of Europeana agents completed.')


unitTest()
    
