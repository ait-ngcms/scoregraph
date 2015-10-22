"""
A collection of commonly use data manipulation procedures.

"""

import json
import os
import codecs
from simplejson import JSONDecodeError
import sys
import requests
import glob


SLASH = '/'
UNDERSCORE = '_'
BLANK = ' '
JSON_EXT = '.json'
XML_EXT = '.xml'

# Mapped authors CSV file
AUTHOR_NAME_COL = 3
VIAF_ID_COL = 7

# Viaf compositions CSV file
VIAF_COMPOSITIONS_CSV_AUTHOR_ID_COL = 0
VIAF_COMPOSITIONS_CSV_COMPOSITION_TITLE_COL = 3

# JSON folders
WIKIDATA_AUTHOR_DATA_DIR = 'data/wikidata_author_data_dir'

# Neo4j
RELATION_AUTHOR_TO_COMPOSITION = 'has_composition'
COMPOSITION_AUTHOR_ID_HEADER = 'author id'
COMPOSITION_TITLE_HEADER = 'title'
AUTHOR_VIAF_ID_HEADER = 'viaf'


wikidata_author_fieldnames = [
    'gnd'
    , 'wikidata'
    , 'onb'
    , 'name'
    , 'genre'
    , 'occupation'
    , 'freebase'
    , AUTHOR_VIAF_ID_HEADER
    , 'bnf'
    , 'nkc'
    , 'nta'
    , 'imslp'
    , 'dbpedia'
    , 'music_brainz_artist_id'
]

viaf_compositions_fieldnames = [
    COMPOSITION_AUTHOR_ID_HEADER
    , 'author name'
    , 'work id'
    , COMPOSITION_TITLE_HEADER
]


def progress(progress=0):

    progress = round(progress * 100)
    print("\n*** Progress: {0}% ***".format(progress))


# I/O handling

def read_records(inputfiles):

    for filename in inputfiles:
        with codecs.open(filename, 'r', 'utf-8') as in_file:
            data = in_file.read()
            yield (filename, data)


def ensure_directory(outputdir):

    """Make sure that output directory exists"""
    if not os.path.exists(outputdir):
        print("Creating directory", outputdir)
        os.makedirs(outputdir)


def write_json_file(outputdir, filename, data):

    ensure_directory(outputdir)
    with codecs.open(outputdir + SLASH + filename, "w", 'utf-8') as out_file:
            json.dump(data, out_file, sort_keys=True, indent=4,
                      ensure_ascii=False, encoding='utf-8')

def write_xml_file(outputdir, filename, data):

    ensure_directory(outputdir)
    file = open(outputdir + SLASH + filename + XML_EXT, "w")
    file.write(data)
    file.close()


def read_json_file(inputfile):

    with open(inputfile) as data_file:
        data = json.load(data_file)
    return data


# this method cleans up a temporary directory
# it is employed before new analysis starts
def cleanup_tmp_directories(folder):

    for the_file in os.listdir(folder):
        file_path = os.path.join(folder, the_file)
        try:
            if os.path.isfile(file_path):
                os.unlink(file_path)
        except Exception, e:
            print e


def toByteStr(text):

    #print 'toByteStr:', text
    res = ''
    if text and text != 'NoneType':
       res = text.encode('utf8', 'ignore')
    return res


def validate_response_json(response):
    return validate_json(response.content)


def validate_json(data):

    json_data = None
    try:
        #print 'response.content', response.content
        #print 'tmp response.content', response.content.replace('[]','"None":""')
        tmp = validate_json_str(data)
        json_data = json.loads(tmp)
    except JSONDecodeError as jde:
        print 'JSONDecodeError. Response author data:', data, jde
    except UnboundLocalError as ule:
        print 'UnboundLocalError. Response author data:', data, ule
    except:
        print 'Response json:', data
        print 'Unexpected error:', sys.exc_info()[0]
    print 'json_data:', json_data
    return json_data


# This method validates JSON string and replaces empty keys []
def validate_json_str(data):

    try:
        tmp = data.replace('[]','"None":""')
        json_data_str = tmp
    except JSONDecodeError as jde:
        print 'JSONDecodeError. Response author data:', data, jde
    except:
        print 'Response json:', data
        print 'Unexpected error:', sys.exc_info()[0]
    print 'json_data_str:', json_data_str
    return json_data_str


def process_http_query(query):

    r = requests.get(query)
    if(r.status_code != 200):
        print('Request error:', r.url)
    return r


def is_stored_as_json_file(path):

    response_json = None
    inputfile = glob.glob(path)
    if(inputfile):
        print 'exists:', inputfile
        #response_content = read_json_file(inputfile[0])
        #response_json = json.loads(response_content)
        response_json = read_json_file(inputfile[0])
    return response_json


def find_longest_substring(string1, string2):
    """ returns the longest common substring from the beginning of string1 and string2 """
    def _iter():
        for a, b in zip(string1, string2):
            if a == b:
                yield a
            else:
                return

    return ''.join(_iter())


def longest_substring_finder(string1, string2):

    answer = ""
    len1, len2 = len(string1), len(string2)
    #for i in range(len1):
    #    match = ""
    #    for j in range(len2):
    #       if (i + j < len1 and string1[i + j] == string2[j]):
    #            match += string2[j]
    #        else:
    #            if (len(match) > len(answer)): answer = match
    #            match = ""
    match = ""
    i = 0
    for j in range(len2):
        if (i + j < len1 and string1[i + j] == string2[j]):
            match += string2[j]
#        else:
#            if (len(match) > len(answer)): answer = match
#            match = ""
#    return answer
    return match


def find_longest_substring_from_list(list):

    parent = ''
    if len(list) > 0:
        parent = list[0]
    for substring in list:
#        parent = longest_substring_finder(parent,substring)
        parent = find_common_substring(parent,substring)
#        parent = find_longest_substring(parent,substring)
    return parent


def long_substr(data):
    substr = ''
    if len(data) > 1 and len(data[0]) > 0:
        for i in range(len(data[0])):
            for j in range(len(data[0])-i+1):
                if j > len(substr) and all(data[0][i:i+j] in x for x in data):
                    substr = data[0][i:i+j]
    return substr


def find_common_substring(str1,str2):

    res = ''
    if len(str1) > 0 and len(str2) > 0:
        for i in range(len(str2)):
            if (i < len(str1) and i < len(str2) and str1[i] == str2[i]):
                res += str1[i]
            else:
                break
    res = check_parent_at_least_a_word(str1, str2, res)
    return res


def check_parent_at_least_a_word(substr, substr2, parent):

    # check whether resulting string is at least one word
    if len(parent) > 0:
        if len(parent) < get_word_len(substr) or len(parent) < get_word_len(substr2):
            parent = ''
    return parent


def get_word_len(str):

    res = 0
    # check whether given string is a word and return word length
    if str != '':
        if ' ' in str:
            res = str.index(" ")
        else:
            res = len(str)
    return res


def convert_json_dict_to_string(json_dict):

    return json.dumps(json_dict)
