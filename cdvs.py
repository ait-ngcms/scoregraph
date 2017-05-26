#!/usr/bin/env python
"""
Script for finding similar Europeana images using CDVS library: http://pacific.tilab.com/www/mpeg-cdvs/index.html

Annotations should be generated for CDVS input.

+++ Folder structure +++

E:\app\europeana-client\image\demo  // contains Europeana image collections


+++ Commands +++

CDVS descriptor extraction module.
=================================
Usage:
  extract <images> <mode> <dataset path> <annotation path>  [-p parameters] [-he
lp]
where:
  images - image files to process (text file, one file name per line)
  mode (0..6) - sets the encoding mode to use
  dataset path - the root dir of the CDVS dataset of images
  annotation path - the root dir of the CDVS annotation files
options:
  -p parameters: text file containing initialization parameters for all modes
  -help or -h: help

OUTPUT in dataset folder: filename.DB.cdvs


CDVS descriptor matching module.
===============================
Usage:
  match <matching-pairs> <non-matching-pairs> <mode> <dataset path> <annotation
path>  [-j] [-t|x tracefile] [-r refmode] [-p paramfile] [-q queryparamfile] [-o
] [-m N][-h]
Parameters:
  matching-pairs - list of matching pairs of images
  non-matching-pairs - list of non-matching pairs of images
  mode (0..n) - the decoding mode to use (described in the parameters file)
  dataset path - the root dir of the CDVS dataset of images
  annotation path - the root dir of the CDVS annotation files
Options:
  -j -jaccard : test accuracy of localization of matches using the full Jaccard
index
  -t -trace filename: trace all results in a text file
  -x -xmltrace filename: trace all results in an XML file
  -r -refmode : reference mode (if different from query mode)
  -p -params parameters: text file containing initialization parameters for all
modes
  -q -queryparams parameters: text file containing initialization parameters for
 queries (not for references - used only in interoperability tests)
  -o -oneway: use one-way local matching (instead of two-way local matching whic
h is the default)
  -m -matchtype N: use match type N, where N can be one of the following values:

     0: ignore global if local match
     1: compute both local and global matching scores
     2: compute only local matching score
     3: compute only global matching score
  -h -help: show help

OUTPUT in stdout: similarity scores


CDVS database index generation module.
=====================================
usage:
  makeIndex <images> <index> <mode> <datasetPath> <annotationPath> [-h]
where:
  images - database images (text file, 1 file name per line) to be indexed
  index - name of index file (or multiple index files) to be generated
  mode (0..n) - sets the encoding mode to use to build the database
  dataset path - the root dir of the CDVS dataset of images
  annotation path - the root dir of the CDVS annotation files
  -help or -h: help

OUTPUT in dataset folder: index-file-name.local index-file-name.global


CDVS retrieval module.
=====================
Usage:
  retrieve <index> <ground-truth-annotations> <mode> <datasetPath> <annotationPa
th> [-o] [-p paramfile] [-t tracefile]
Where:
  index - database index file to be used for retrieval
  ground-truth-annotations - query images and corresponding (ground truth)
        matches in the database; this is a text file, containing per each line,
        the following records:
           <query image> <matching image 1> ... <matching image N>
  mode (0..n) - sets the encoding mode to use (described in the parameters file)

  dataset path - the root dir of the CDVS dataset of images
  annotation path - the root dir of the CDVS annotation files
Options:
  -o -oneway: use one-way matching (instead of two-way matching which is the def
ault)
  -p paramfile: text file containing initialization parameters for all modes
  -t -tracefile: trace all results in a trace file containing
         ranked lists of results retrieved per each query;
         it must be in same format as ground truth data.
  -help or -h: help

OUTPUT in tracefile in root folder:


+++ Invocation +++
$ python cdvs.py inputdir -u exract
"""

import argparse
import os
import sys
import subprocess
import time

# directory structure
from os import walk

import common


# Folders
CDVS_BIN_FOLDER = "C:\\git\\mpeg\\CDVS\\CDVS_evaluation_framework\\bin\\all_projects\\bin\\x64_Release\\"


# CDVS parameters
MODE = '0' # (0..6) - sets the encoding mode to use
IMAGE_DIR = "image"  # root image directory
IMAGE_LIST = "image_list.txt" # image files to process(text file, one file name per line)
DATASET_PATH = "demo" # the root directory of the CDVS dataset of images
ANNOTATION_PATH = "annotation" # the root dir of the CDVS annotation files
FEATURES_PATH = "features" # the location of feature files after extract command execution with extension DB.cdvs

# Commands
EXTRACT = 'extract'
INDEX_CMD = 'makeIndex'
INDEX = 'index'

# Definitions
ALLOWED_EXTENSIONS = ['.jpg']


def execute_command_using_cmd(param_list):

    proc = subprocess.Popen(param_list, stdout=subprocess.PIPE)
    (out, err) = proc.communicate()
    print (out)
    return out

# Example:
# C:\git\mpeg\CDVS\CDVS_evaluation_framework\bin\all_projects\bin\x64_Release\extract>extract.exe
# test-files.txt 0 E:\app\europeana-client\image\demo\07101 E:\app\europeana-client\image\annotation
def extract_cdvs_features(inputdir, image_list_file, dataset_path):

    image_names = [line.rstrip('\n') for line in open(inputdir + "\\" + ANNOTATION_PATH + "\\" + image_list_file)]
    if not exists_file(dataset_path + "\\" + image_names[0].replace(".jpg", ".cdvs")):
        exe = CDVS_BIN_FOLDER + "\\" + EXTRACT + "\\" + EXTRACT + ".exe"
        param_list = [exe, image_list_file, MODE, dataset_path, inputdir + "\\" + ANNOTATION_PATH]
        execute_command_using_cmd(param_list)
    else:
        print 'CDVS features for dataset ' + dataset_path + ' already exist.'


def exists_file(path):

    response = False
    import os.path
    try:
        if os.path.isfile(path):
            response = True
    except Exception as ex:
        print ex

    return response


def extract(inputdir):

    # Generate image path list
    if IMAGE_DIR in inputdir:
        image_collection_dirs = os.listdir(inputdir + "\\" + DATASET_PATH)
        for image_collection_dir in image_collection_dirs:
            image_files = []
            dataset_dir = inputdir + "\\" + DATASET_PATH + "\\" + image_collection_dir
            image_files.extend(common.extract_file_names_from_dir(dataset_dir))
            image_list_file = image_collection_dir + "-" + IMAGE_LIST
            # take only allowed extensions e.g. JPG
            image_files = filter(None, [image_file if image_file.endswith(tuple(ALLOWED_EXTENSIONS)) else None for image_file in image_files])
            common.write_txt_file_from_list(inputdir + "\\" + ANNOTATION_PATH, image_list_file, image_files)
            # Extract features
            extract_cdvs_features(inputdir, image_list_file, dataset_dir)
    else:
        print 'Error. ' +  DATASET_PATH + ' folder is missing.'

    print '+++ CDVS feature extraction completed +++'


# Example:
# C:\git\mpeg\CDVS\CDVS_evaluation_framework\bin\all_projects\bin\x64_Release\makeIndex>makeIndex.exe
# test-files.txt index 0 E:\app\europeana-client\image\demo\07101 E:\app\europeana-client\image\annotation
def index_images(inputdir, image_list_file, dataset_path):

    image_names = [line.rstrip('\n') for line in open(inputdir + "\\" + ANNOTATION_PATH + "\\" + image_list_file)]
    if not exists_file(dataset_path + "\\" + INDEX + ".local"):
        exe = CDVS_BIN_FOLDER + "\\" + INDEX_CMD + "\\" + INDEX_CMD + ".exe"
        param_list = [exe, image_list_file, INDEX, MODE, dataset_path, inputdir + "\\" + ANNOTATION_PATH]
        execute_command_using_cmd(param_list)
    else:
        print 'CDVS index files for dataset ' + dataset_path + ' already exist.'


def index(inputdir):

    # Generate image path list
    if IMAGE_DIR in inputdir:
        image_collection_dirs = os.listdir(inputdir + "\\" + DATASET_PATH)
        for image_collection_dir in image_collection_dirs:
            image_files = []
            dataset_dir = inputdir + "\\" + DATASET_PATH + "\\" + image_collection_dir
            image_files.extend(common.extract_file_names_from_dir(dataset_dir))
            image_list_file = image_collection_dir + "-" + IMAGE_LIST
            # Index images
            index_images(inputdir, image_list_file, dataset_dir)
    else:
        print 'Error. ' +  DATASET_PATH + ' folder is missing.'

    print '+++ CDVS indexing completed +++'


def cleanup(inputdir, dirnames):

    # clean up directories
#    common.cleanup_tmp_directories(raw_path)
#    os.remove('/summary_' + mode_normalized + '.csv')
    pass

    print '+++ CDVS cleanup completed +++'


# Main analyzing routine

def analyze_images(inputdir, use_case):

    start = time.time()
    print("Analysing '" + inputdir + "' images...")

    if use_case == EXTRACT:
        extract(inputdir)

    if use_case == INDEX:
        index(inputdir)

    end = time.time()
    print 'Calculation time:', end - start


# Command line parsing

if __name__ == '__main__':

    parser = argparse.ArgumentParser(
                    description="Finding similar Europeana images using CDVS library.")
    parser.add_argument('inputdir', type=str, default="E:\\app-test\\europeana-client\\image", help="Input image files to be processed")
    parser.add_argument('-u', '--use_case', type=str, nargs='?',
                        default="data/mapping.csv",
                        help="Analysis use cases in given order, such as 'all', 'extract', 'match', 'index', 'retrieve', 'cleanup'")

    if len(sys.argv) < 2:
        parser.print_help()
        sys.exit(1)

    args = parser.parse_args()
    analyze_images(args.inputdir, args.use_case)
