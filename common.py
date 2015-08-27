"""
A collection of commonly use data manipulation procedures.

"""

import json
import os
import codecs


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
