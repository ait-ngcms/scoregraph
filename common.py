"""
A collection of commonly use data manipulation procedures.

"""

import json
import os
import codecs
from simplejson import JSONDecodeError
import sys
import requests


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
    with codecs.open(outputdir + "/" + filename, "w", 'utf-8') as out_file:
            json.dump(data, out_file, sort_keys=True, indent=4,
                      ensure_ascii=False, encoding='utf-8')


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


