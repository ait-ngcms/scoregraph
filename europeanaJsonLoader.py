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
COINS_INPUT_FILE = 'coins_in_europeana.txt'
JSON_EXT = '.json'
OUTPUT_DIR = 'output'
OUTPUT_DIR_2 = 'output2'
OUTPUT_DIR_COINS = 'output-coins'
SLASH = '/'

TEST_URL = "http://www.europeana.eu/portal/en/search.json?view=grid&q=numismatic&f%5BMEDIA%5D%5B%5D=true&f%5BTYPE%5D%5B%5D=IMAGE&per_page=96"
OUTPUT_DIR_URL = 'output-url'
URLS_INPUT_FILE = 'coins_urls.txt'


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


def save_json_list(outputdir, filename, data):

    return save_json_list_by_field(outputdir, filename, data, 'items')


def save_json_list_by_field(outputdir, filename, data, fieldname):

    res = True

    try:
        response = json.loads(data)
        items = response[fieldname]
        for item in items:
            jsonfile = None
            if 'id' in item:
                jsonfile = filename.replace(JSON_EXT, item['id'].replace('/','-') + JSON_EXT)
            if 'title' in item:
                jsonfile = filename.replace(JSON_EXT, item['title'] + JSON_EXT)
            jsonfile = jsonfile.replace(" ", "").replace("\\",'-').replace(":", "-")
            write_json_file(outputdir, jsonfile, item)

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


def loadCoinsTest():

    print('Loading of Europeana coins started ...')

    coins = []
    COIN_ROWS = 100

    # read coins links from input file
    with open(COINS_INPUT_FILE) as f:
        coin = f.readlines()
        # remove whitespace characters
        coins = [x.strip() for x in coin]

    count = 0
    for coin in coins:
        location_id = None
        url_address = None
        try:
            url_address = "http://www.europeana.eu/api/v2/search.json?wskey=api2demo&profile=rich&rows=" + str(COIN_ROWS) + "&query=%22/" + \
                          coin.replace("http://data.europeana.eu/place/base/", "place/base/") + "%22%20AND%20coin"
            location_id = coin.split(SLASH)[-1]
            filename = str(location_id) + JSON_EXT

            if not is_stored_as_json_file(OUTPUT_DIR_COINS + SLASH + filename):
                content = urllib.urlopen(url_address).read()
                if not save_json_list(OUTPUT_DIR_COINS, filename, content):
                    save_text(OUTPUT_DIR_COINS, filename, content)

        except Exception as ex:
            print('Error loading coin:', coin, 'id:', location_id, 'url:', url_address, 'count:', count)
            print(ex)

        count = count + 1

    assert(len(coins) > 0)
    print('Loading of Europeana coins completed.')


def loadUrlsTest(url):

    print('Loading of Europeana URLs started ...')

    items = []

    # read URLs from input file
    with open(URLS_INPUT_FILE) as f:
        item = f.readlines()
        # remove whitespace characters
        items = [x.strip() for x in item]

    for idx, item in enumerate(items):
        loadUrl(url, str(idx) + '-')

    print('Loading of Europeana URLs completed.')


def loadUrl(url, result_filename):

    print('Loading of Europeana URL:', url)

    try:
        filename = result_filename + JSON_EXT

        content = urllib.urlopen(url).read()
        if not save_json_list_by_field(OUTPUT_DIR_URL, filename, content, 'search_results'):
            save_text(OUTPUT_DIR_URL, filename, content)

    except Exception as ex:
        print('Error loading URL:', url, 'result filename:', result_filename, 'url:', url)
        print(ex)

    print('Loading of Europeana URL', url, 'completed.')


#unitTest()
#loadCoinsTest()
loadUrlsTest(TEST_URL)
