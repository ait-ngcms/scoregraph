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
  -j -jaccard : test accuracy of localization of matches using the full Jaccard index
  -t -trace filename: trace all results in a text file
  -x -xmltrace filename: trace all results in an XML file
  -r -refmode : reference mode (if different from query mode)
  -p -params parameters: text file containing initialization parameters for all modes
  -q -queryparams parameters: text file containing initialization parameters for
      queries (not for references - used only in interoperability tests)
  -o -oneway: use one-way local matching (instead of two-way local matching which is the default)
  -m -matchtype N: use match type N, where N can be one of the following values:

     0: ignore global if local match
     1: compute both local and global matching scores
     2: compute only local matching score
     3: compute only global matching score
  -h -help: show help

OUTPUT in stdout: similarity scores
e.g.
index, nmatched, inliers,    weight,  w-thresh,  g-thresh,   g-score,     score, [bounding box]
    1       300      299 299.000000   1.575000   7.235000 279.402283   1.000000
    2        67        0   0.000000   1.575000   7.235000   9.250980   1.000000
    3        61        0   0.000000   1.575000   7.235000   9.039987   1.000000

The meaning of the fields is as following:
   index:     index of document in a folder
   nmatched:  actual number of matched points
   inliers:   indicates the number of pairs which actually match according to the geometric verification
   weight:    local score - matching score provided by local descriptors
   w-thresh:  local threshold used for weighted matching of keypoints  (query_params.wmThreshold)
   g-thresh:  global threshold used for matching the global descriptor (query_params.gdThreshold)
   g-score:   matching score provided by global descriptor
   score:     final normalized score (in a range from 0 to 1)

The list of parameters that can be changed is the following:

int descLength;                    length in bytes of the CDVS descriptor (i.e. 512, 1024, 2048)
int resizeMaxSize;                 maximum size of one side of the image
int blockWidth;                    coordinate coding: spatial resolution of the coordinates (max int ctxTableIdx; coordinate coding: index of the context table to use
char modeExt[40];                  descriptor extension
unsigned int selectMaxPoints;      feature extraction: max number of points used to describe an unsigned int numRelevantPoints; feature extraction: number of points considered relevant in the float ratioThreshold; DISTRAT: threshold for descriptor matching
unsigned int minNumInliers;        DISTRAT: min number of inliers after the geometric check
double wmThreshold;                Weighted matching threshold
double wmThreshold2Way;            Two way matching weighted threshold
double wmMixed;                    Weighted matching threshold for mixed cases
double wmMixed2Way;                Two way weighted matching threshold for mixed cases
int debugLevel;                    0 = off, 1 = on (quiet), 2 = on (verbose), 3 = verbose + dump int ransacNumTests; RANSAC: number of iterations in RANSAC
float ransacThreshold;             RANSAC: distortion threshold to be used by RANSAC
unsigned int chiSquarePercentile;  percentile used in DISTRAT for Chi-square computation
int retrievalLoops;                number of loops performed in the final stage of the retrieval double wmRetrieval; Weighted matching threshold for retrieval
double wmRetrieval2Way;            Two way weighted matching threshold for retrieval
int retrievalMaxPoints;            max number of points used in the retrieval experiment
int queryExpansionLoops;           number of query expansion loops to perform in the retrieval experiment
float scfvThreshold;               threshold value to control the sparsity of scfv vector
bool hasVar;                       indicates if using the gradient vector w.r.t the variance of float locationBits; average bits per key point to encode location information;
bool hasBitSelection;              indicates if the Global Descriptor uses the bit selection algorithm float gdThreshold; global descriptor threshold
float gdThresholdMixed;            global descriptor threshold for mixed cases

The example parameter file:

parameters.txt

[Mode = 0]
selectMaxPoints = 600;
retrievalMaxPoints = 600;
gdThreshold = 5.0;
wmThreshold = 1.5;


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

OUTPUT in tracefile in root folder: e.g. for O_102.jpg it is O_344.jpg O_393.jpg O_226.jpg O_157.jpg O_91.jpg


+++ Invocation +++

$ python cdvs.py <inputdir> -u <use case>

Example: E:\app-test\europeana-client\image -u extract
E:\app-test\testcollection -u all-test -q O_446.jpg

To search among all documents in test collection with CSVS feature extraction (-m F) taking parameters from
configuration file (-p params.txt):
E:\app-test\testcollection -u all-test -m F -p params.txt

+++ Workflow for audio search API +++

1. Generate file names of type <collection_id>_<file_id>.jpg using Europeana ID from demo.csv
E:\app-test\testcollection -u generate-filenames

2. Collect all images in a test collection folder. Names of the Europeana images should be in form
<collection_id>_<file_id>.jpg

3. For all files in a test collection: extract features, index and match.
E:\app-test\testcollection -u all-test

4. Search collection from API
E:\app-test\testcollection -u search-collection -q 07101_O_446.jpg

5. Rename Europeana files
E:\app-test\collection-all -u europeana-rename
or with CDVS feature extraction
E:\app-test\collection-all -u europeana-rename -m F

"""

import argparse
import os
import sys
import subprocess
import time

import common

import json
import csv


# Folders
CDVS_BIN_FOLDER = "C:\\git\\mpeg\\CDVS\\CDVS_evaluation_framework\\bin\\all_projects\\bin\\x64_Release\\"

#Files
RETRIEVAL_GROUND_TRUTH_FILE = 'ground-truth-annotations.txt'
RETRIEVAL_OUTPUT_FILE = 'retrieval-output.txt'
MATCH_OUTPUT_FILE = 'match-output.txt'
MATCHING_PAIRS_FILE = 'matching-pairs.txt'
NON_MATCHING_PAIRS_FILE = 'non-matching-pairs.txt'
SEARCH_RESULT_FILE = 'search-result-file.html'
DEMO_CSV_FILE = 'demo.csv'
DEMO_EXT_CSV_FILE = 'demo-ext.csv'

# CDVS parameters
MODE = '0' # (0..6) - sets the encoding mode to use
IMAGE_DIR = "image"  # root image directory
IMAGE_LIST = "image_list.txt" # image files to process(text file, one file name per line)
DATASET_PATH = "demo" # the root directory of the CDVS dataset of images
ANNOTATION_PATH = "annotation" # the root dir of the CDVS annotation files
FEATURES_PATH = "features" # the location of feature files after extract command execution with extension DB.cdvs

ORIGINAL_EUROPEANA_STRUCTURE = "E:\\app-test\\europeana-client\\image\\demo"

# Commands
ALL_TEST = 'all-test' # test the whole extract-index-match-htmlview process
EXTRACT_MATCH_COLLECTION = 'extract-match-collection' # test extract and match collection for a query image
SEARCH_COLLECTION = 'search-collection' # test searching collection for a query image
EXTRACT = 'extract'
INDEX_CMD = 'makeIndex'
INDEX = 'index'
RETRIEVE = 'retrieve'
MATCH = 'match'
GENERATE_FILENAMES = 'generate-filenames'
EUROPEANA_RENAME = 'europeana-rename'


# Definitions
JPG_EXT = '.jpg'
ALLOWED_EXTENSIONS = [JPG_EXT]

# Column positions in Europeana demo.csv
PATH_POS = 0
EUROPEANA_ID_POS = 1
TITLE_POS = 2
URI_POS = 3
FILENAME_POS = 4

# Europeana file name positions
COLLECTION_POS_IN_EUROPEANA_NAME = 0
FILENAME_POS_IN_EUROPEANA_NAME = 1


def execute_command_using_cmd(param_list):

    proc = subprocess.Popen(param_list, stdout=subprocess.PIPE)
    (out, err) = proc.communicate()
    print (out)
    return out


def execute_command_using_cmd_from_dir(param_list, dir):

    proc = subprocess.Popen(param_list, cwd=dir, stdout=subprocess.PIPE)
    (out, err) = proc.communicate()
    print (out)
    return out

# Example:
# C:\git\mpeg\CDVS\CDVS_evaluation_framework\bin\all_projects\bin\x64_Release\extract>extract.exe
# test-files.txt 0 E:\app\europeana-client\image\demo\07101 E:\app\europeana-client\image\annotation
def extract_cdvs_features(annotation_dir, image_list_file, dataset_path):

    image_names = [line.rstrip('\n') for line in open(annotation_dir + "\\" + image_list_file)]
    if not exists_file(dataset_path + "\\" + image_names[0].replace(".jpg", ".cdvs")):
        exe = CDVS_BIN_FOLDER + "\\" + EXTRACT + "\\" + EXTRACT + ".exe"
        param_list = [exe, image_list_file, MODE, dataset_path, annotation_dir]
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
            dataset_dir = inputdir + "\\" + DATASET_PATH + "\\" + image_collection_dir
            image_files = get_allowed_image_file_names_from_dir(dataset_dir)
            image_list_file = image_collection_dir + "-" + IMAGE_LIST
            common.write_txt_file_from_list(inputdir + "\\" + ANNOTATION_PATH, image_list_file, image_files)
            # Extract features
            extract_cdvs_features(inputdir + "\\" + ANNOTATION_PATH, image_list_file, dataset_dir)
    else:
        print 'Error. ' +  DATASET_PATH + ' folder is missing.'

    print '+++ CDVS feature extraction completed +++'


def extract_collection(inputdir, mode):

    print 'extract features with mode:', mode
    # Generate image path list
    image_files = []
    image_files.extend(common.extract_file_names_from_dir(inputdir))
    image_list_file = os.path.basename(os.path.normpath(inputdir)) + "-" + IMAGE_LIST
    # take only allowed extensions e.g. JPG
    image_files = filter(None, [image_file if image_file.endswith(tuple(ALLOWED_EXTENSIONS)) else None for image_file in image_files])
    if mode == 'F':
        common.write_txt_file_from_list(inputdir, image_list_file, image_files)
        # Extract features
        extract_cdvs_features(inputdir, image_list_file, inputdir)

    print '+++ CDVS feature extraction completed +++'

    return image_files


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


def retrieve(inputdir):

    # Generate image path list
    if IMAGE_DIR in inputdir:
        image_collection_dirs = os.listdir(inputdir + "\\" + DATASET_PATH)
        for image_collection_dir in image_collection_dirs:
            dataset_dir = inputdir + "\\" + DATASET_PATH + "\\" + image_collection_dir
            # Retrieve similar images
            retrieve_similar_images(inputdir, dataset_dir)
    else:
        print 'Error. ' +  DATASET_PATH + ' folder is missing.'

    print '+++ CDVS retrieval completed +++'


def check_query_file_is_in_folder(dataset_dir, inputfile):

    res = False
    if exists_file(inputfile):
        main_image = extract_image_list_from_input_file(inputfile)[0]
        if main_image in common.extract_file_names_from_dir(dataset_dir):
            res = True
    else:
        print 'Input file does not exist. Matching requires existing of file:', inputfile
    return res


def read_collection_summary(inputdir):

    summary = None

    demo_file = inputdir + "\\" + DEMO_CSV_FILE
    if os.path.isfile(demo_file):
        summary = read_demo_summary(demo_file)
    return summary


def match(inputdir):

    print '+++ CDVS match started +++'
    # Generate image path list
    if IMAGE_DIR in inputdir:
        image_collection_dirs = os.listdir(inputdir + "\\" + DATASET_PATH)
        for image_collection_dir in image_collection_dirs:
            dataset_dir = inputdir + "\\" + DATASET_PATH + "\\" + image_collection_dir
            summary = read_collection_summary(inputdir)
            # Match images
            match_images(inputdir, dataset_dir, summary)
    else:
        print 'Error. ' +  DATASET_PATH + ' folder is missing.'

    print '+++ CDVS match completed +++'


def generate_filenames(inputdir):

    print '+++ CDVS generate file names started +++'
    # Generate filenames in CSV file
    demo_file = inputdir + "\\" + DEMO_CSV_FILE
    if os.path.isfile(demo_file):
        summary = read_demo_summary(demo_file)
        add_filename_column(summary, inputdir + "\\" + DEMO_EXT_CSV_FILE)
    else:
        print 'Error. ' +  DEMO_CSV_FILE + ' is missing.'

    print '+++ CDVS generate file names completed +++'


def europeana_rename(inputdir):

    print '+++ Aggregation of all Europeana files in one folder renaming each file by adding collection name started +++'

    # Generate image path list
    demo_file = inputdir + "\\" + DEMO_EXT_CSV_FILE
    if os.path.isfile(demo_file):
        summary = read_demo_summary(demo_file)
        move_files(inputdir, summary)
    else:
        print 'Error. ' + DEMO_CSV_FILE + ' is missing.'

    print '+++ Aggregation of all Europeana files completed +++'


def all_test(inputdir, mode, params):

    print '+++ CDVS all test started +++'

    image_files = extract_collection(inputdir, mode)

    print '+++ CDVS all test - collection extracted +++'

    summary = read_collection_summary(inputdir)
    for query in image_files:
        match_collection(inputdir, query, params, summary)

    print '+++ CDVS all test completed +++'


def extract_match_collection(inputdir, query, params):

    print '+++ CDVS extract and match collection test started +++'

    extract_collection(inputdir)
    summary = read_collection_summary(inputdir)
    match_collection(inputdir, query, params, summary)

    print '+++ CDVS extract and match collection test completed +++'


def search_collection(inputdir, query, params, summary):

    print '+++ CDVS search collection test started +++'

    res = search_similar_in_collection(inputdir, query, params, summary)
    res_json = json.dumps(res)
    print "#search result view#", res_json, "#search result view#"

    print '+++ CDVS search match collection test completed +++'


def extract_image_list_from_input_file(filepath):

    lines = [line.rstrip('\n') for line in open(filepath)]
    images = lines[0].split(" ")
    return images


def extract_score_from_match_output_file(inputdir, match_output_file, images, summary):

    INDEX_POS         = 0
    FEATURES_NUM_POS  = 1
    INLIERS_POS       = 2
    WEIGHT_POS        = 3
    W_THRES_POS       = 4
    G_THRES_POS       = 5
    GSCORE_POS        = 6
    SCORE_POS         = 7

    SORT_BY_GSCORE = 3
    SORT_BY_FINAL_SCORE = 4
    SORT_BY_LSCORE = 12
    SORT_BY_CUSTOM_SCORE = 15

    res = []
#    summary = None

#    demo_file = inputdir + "\\" + DEMO_CSV_FILE
#    if os.path.isfile(demo_file):
#        summary = read_demo_summary(demo_file)

    lines = [line.rstrip('\n') for line in open(match_output_file)][2:] # remove headers - first two lines
    for idx, line in enumerate(lines):
        values = line.split()
        index = values[INDEX_POS]
        features_num = values[FEATURES_NUM_POS]
        inliers = values[INLIERS_POS]
        weight = values[WEIGHT_POS]
        w_thres = values[W_THRES_POS]
        g_thres = values[G_THRES_POS]
        gscore = values[GSCORE_POS]
        score = values[SCORE_POS]
        file_name = None
        path = None
        europeana_id = None
        title = None
        uri = None
        custom_score = None

        try:
            input_file = images[idx].split()[1]
            file_name, path, europeana_id, title, uri = extract_europeana_fields(summary, input_file)
        except Exception as ex:
            print 'Error for input file:', input_file, ex
        if not score.startswith("0.0"):
            custom_score = (float(weight) + float(gscore)) * float(features_num)
            #res.append([idx, images[idx], features_num, gscore, calculated_score, file_name, path, europeana_id, title, uri])
            res.append(
                [idx, images[idx], features_num, gscore, score, file_name, path, europeana_id, title, uri, index,
                 inliers, weight, w_thres, g_thres, str(custom_score)
                ]
            )

#    res = sorted(res, key=lambda x:float(x[SORT_BY_FINAL_SCORE]), reverse=True)
#    res = sorted(res, key=lambda x:float(x[SORT_BY_GSCORE]), reverse=True)
#    res = sorted(res, key=lambda x:float(x[SORT_BY_LSCORE]), reverse=True)
#    res = sorted(res, key=lambda x: (float(x[SORT_BY_LSCORE]) and float(x[SORT_BY_GSCORE])), reverse=True)
    res = sorted(res, key=lambda x: float(x[SORT_BY_CUSTOM_SCORE]), reverse=True)

    return res


# enrich results with Europeana specific fields from demo.csv
def extract_europeana_fields(summary, query_file_name):

    for row in summary:
        europeana_id = row[EUROPEANA_ID_POS]
        file_name = europeana_id[1:].replace('/', '_') + JPG_EXT
        if file_name == query_file_name:
            return file_name, row[PATH_POS], row[EUROPEANA_ID_POS], row[TITLE_POS], row[URI_POS]


def generate_html_view(dataset_path, annotation_path, score_dict):

    IMAGE_PAIR_POS = 1

    main_image = score_dict[0][IMAGE_PAIR_POS].split()[0]

    html_output_file = annotation_path + main_image.replace(".", "-") + "-" + SEARCH_RESULT_FILE

    if not exists_file(html_output_file):

        html_content = ''

        html_prefix = ("<html>\n\t<head>\n\t<title> CDVS - Image Retrieval Demo </title>\n\t</head>\n\t<div class=\"wrapper\" style=\"min-height: 900px;\">\n\t"
        + "<table>\n\t<tr>\n\t<td>\n\t<table>\n\t<tr>\n\t<td>\n\t<div style=\"display: none;\" id=\"advSearch\" align=\"center\"><img id=\"queryImage\" src=\"images/"
        + main_image + "\" alt = \"\" height=\"64\" align=\"middle\"></div>\n\t</td>\n\t</tr>\n\t</table>\n\t</td>\n\t</tr>\n\t</table>\n\t"
        + "<h1>CDVS similarity search results (" + str(len(score_dict)) + ") for image: " + main_image.split()[0] + "</h1>\n\t"
        + "<div class=\"content\" align=\"center\">\n\t<table border=\"0\" align=\"center\">")


        COLUMN_COUNT = 14 #4
        for idx, score_obj in enumerate(score_dict):

            INDEX_POS         = 1
            FEATURES_NUM_POS  = 2
            G_SCORE_POS       = 3
            SCORE_POS         = 4
            FILE_NAME_POS     = 5
            PATH_POS          = 6
            EUROPEANA_ID_POS  = 7
            TITLE_POS         = 8
            URI_POS           = 9
            INDEX_ORIG_POS    = 10
            INLIERS_POS       = 11
            WEIGHT_POS        = 12
            W_THRES_POS       = 13
            G_THRES_POS       = 14
            CUSTOM_SCORE_POS  = 15

            image_pair = score_obj[IMAGE_PAIR_POS].split()
            key_image = image_pair[0]
            related_image = image_pair[1]
            features_num = str(score_obj[FEATURES_NUM_POS])
            g_score = str(score_obj[G_SCORE_POS])
            score = str(score_obj[SCORE_POS])
            file_name = score_obj[FILE_NAME_POS]
            path = score_obj[PATH_POS]
            europeana_id = score_obj[EUROPEANA_ID_POS]
            title = score_obj[TITLE_POS]
            uri = score_obj[URI_POS]
            index_orig = str(score_obj[INDEX_ORIG_POS])
            inliers = str(score_obj[INLIERS_POS])
            weight = str(score_obj[WEIGHT_POS])
            w_thres = str(score_obj[W_THRES_POS])
            g_thres = str(score_obj[G_THRES_POS])
            custom_score = str(score_obj[CUSTOM_SCORE_POS])

            row_begin = ""
            row_end = ""
            col_count = idx%COLUMN_COUNT
            if col_count == 0:
                row_begin = "\n\t\n\t\t<tr valign=top>\n\t"
            if col_count == COLUMN_COUNT-1:
                row_end = "\t</tr>"
            if uri is not None:
                html_content = (html_content + row_begin + "\t\t<td valign=\"top\">\n\t" + "\t\t\t<div id=\"result_" + score_obj[INDEX_POS]
                + "\" style=\"padding: 5px;\">\n\t\t\t\t\t<div>\n\t\t\t\t\t\t<div>\n\t\t\t\t\t\t\t<a href=\"" + uri + "\" title=\"search similar images\">"
                + "Title: " + str(title) + "</a>&nbsp;(custom score=" + custom_score + ", final score=" + score + ", l-score=" + weight + ", g-score=" + g_score + ", idx=" + str(idx)
                + ", matched points=" + features_num + ", file name=" + str(file_name) + ", path=" + str(path) + ", europeana_id="
                + str(europeana_id) + ", image=" + related_image
                + ", uri=" + str(uri) + ", index_orig=" + index_orig + ", inliers=" + inliers + ", l_thres=" + w_thres + ", g_thres=" + g_thres + ")"
    #            + "\n\t\t\t\t\t\t\t\t<img style=\"background-color: white; border-color: black; border-width: 10;\" src=\"file://" + dataset_path.replace("\\", "/")  + "/"
    #            + related_image + "\" title=\"score: " + features_num + ", " + g_score + "\"/>"
                + "\n\t\t\t\t\t\t\t\t<img style=\"background-color: white; border-color: black; border-width: 10;\" src=\"" + uri
                + "\" title=\"score: " + features_num + ", " + g_score + "\"/>"
                + "<br>\n\t\t\t\t\t\t</div>\n\t\t\t\t\t</div>\n\t\t\t\t</div>\n\t\t\t</td>\n\t" + row_end)

        html_postfix = "\n\t</table>\n\t</div>\n\t</body>\n\t</html>"

        data = html_prefix + html_content + html_postfix

        # create HTML file
        common.write_txt_file_from_string(annotation_path, main_image.replace(".", "-") + "-" + SEARCH_RESULT_FILE, data)


def generate_similar_images_html_view(dataset_path, score_dict):

    IMAGE_PAIR_POS = 1
    main_image = score_dict[0][IMAGE_PAIR_POS].split()[0]

    html_content = ''

    html_prefix = ("<div class=\"wrapper\" style=\"min-height: 900px;\">\n\t"
    + "<table>\n\t<tr>\n\t<td>\n\t<table>\n\t<tr>\n\t<td>\n\t<div style=\"display: none;\" id=\"advSearch\" align=\"center\"><img id=\"queryImage\" src=\"images/"
    + main_image + "\" alt = \"\" height=\"64\" align=\"middle\"></div>\n\t</td>\n\t</tr>\n\t</table>\n\t</td>\n\t</tr>\n\t</table>\n\t"
    + "<h1>CDVS similarity search results (" + str(len(score_dict)) + ") for image: " + main_image.split()[0] + "</h1>\n\t"
    + "<div class=\"content\" align=\"center\">\n\t<table border=\"0\" align=\"center\">")


    COLUMN_COUNT = 4
    for idx, score_obj in enumerate(score_dict):

        INDEX_POS = 1
        FEATURES_NUM_POS = 2
        G_SCORE_POS = 3
        CALCULATED_SCORE = 4
        image_pair = score_obj[IMAGE_PAIR_POS].split()
        key_image = image_pair[0]
        related_image = image_pair[1]
        features_num = score_obj[FEATURES_NUM_POS]
        g_score = score_obj[G_SCORE_POS]
        calculated_score = score_obj[CALCULATED_SCORE]

        row_begin = ""
        row_end = ""
        col_count = idx%COLUMN_COUNT
        if col_count == 0:
            row_begin = "\n\t\n\t\t<tr valign=top>\n\t"
        if col_count == COLUMN_COUNT-1:
            row_end = "\t</tr>"
        html_content = (html_content + row_begin + "\t\t<td valign=\"top\">\n\t" + "\t\t\t<div id=\"result_" + score_obj[INDEX_POS]
        + "\" style=\"padding: 5px;\">\n\t\t\t\t\t<div>\n\t\t\t\t\t\t<div>\n\t\t\t\t\t\t\t<a href=\"\" title=\"search similar images\">"
#        + related_image + "</a>&nbsp;score: " + calculated_score + " (features=" + features_num + ", g-score=" + g_score + ")"
        + related_image + "</a>&nbsp;(features=" + features_num + ", g-score=" + g_score + ")"
        + "\n\t\t\t\t\t\t\t\t<img style=\"background-color: white; border-color: black; border-width: 10;\" src=\"file://" + dataset_path.replace("\\", "/")  + "/"
        + related_image + "\" title=\"score: " + features_num + ", " + g_score + "\"/>"
        + "<br>\n\t\t\t\t\t\t</div>\n\t\t\t\t\t</div>\n\t\t\t\t</div>\n\t\t\t</td>\n\t" + row_end)

    html_postfix = "\n\t</table>\n\t</div>"

    data = html_prefix + html_content + html_postfix

    return data


def get_collection_id_from_path(dataset_path):

    return dataset_path.split("\\")[-1]


# Example:
# C:\git\mpeg\CDVS\CDVS_evaluation_framework\bin\all_projects\bin\x64_Release\retrieve>retrieve.exe
# index ground-truth-annotations.txt 0 C:\app\europeana-client\image\demo\07101 C:\app\europeana-client\image\annotation
# -t retrieval-output.txt
# output is stored in output file in directory where current python script is located
def retrieve_similar_images(inputdir, dataset_path):

    if check_query_file_is_in_folder(dataset_path, RETRIEVAL_GROUND_TRUTH_FILE):
        output_file = get_collection_id_from_path(dataset_path) + "-" + RETRIEVAL_OUTPUT_FILE
        exe = CDVS_BIN_FOLDER + "\\" + RETRIEVE + "\\" + RETRIEVE + ".exe"
        param_list = [exe, INDEX, RETRIEVAL_GROUND_TRUTH_FILE, MODE, dataset_path, inputdir + "\\" + ANNOTATION_PATH, "-t",
                      output_file]
        execute_command_using_cmd(param_list)


# Example:
# C:\git\mpeg\CDVS\CDVS_evaluation_framework\bin\all_projects\bin\x64_Release\match>match.exe
# matching-pairs.txt non-matching-pairs.txt 0 C:\app\europeana-client\demo\07101 C:\app\europeana-client\annotation
# -t match-output.txt
def match_images(inputdir, dataset_path, summary):

    # read outputs of retrieve command
    retrieval_output_file = get_collection_id_from_path(dataset_path) + "-" + RETRIEVAL_OUTPUT_FILE
    if check_query_file_is_in_folder(dataset_path, retrieval_output_file):

        images = extract_image_list_from_input_file(retrieval_output_file)
        main_image = images[0]
        images = [main_image + " " + image for image in images]

        # generate matching pairs file
        common.write_txt_file_from_list(inputdir + "\\" + ANNOTATION_PATH, MATCHING_PAIRS_FILE, images[:-1])

        # generate non matching pairs file
        common.write_txt_file_from_list(inputdir + "\\" + ANNOTATION_PATH, NON_MATCHING_PAIRS_FILE, images[:1])

        matching_output_file = get_collection_id_from_path(dataset_path) + "-" + MATCH_OUTPUT_FILE

        exe = CDVS_BIN_FOLDER + "\\" + MATCH + "\\"  + MATCH + ".exe"
        param_list = [exe, MATCHING_PAIRS_FILE, NON_MATCHING_PAIRS_FILE, MODE, dataset_path, inputdir + "\\" + ANNOTATION_PATH, "-t",
                      matching_output_file]
        execute_command_using_cmd(param_list)

        score_dict = extract_score_from_match_output_file(inputdir, matching_output_file, images, summary)
        generate_html_view(dataset_path, inputdir + "\\" + ANNOTATION_PATH, score_dict)


def get_allowed_image_file_names_from_dir(inputdir):

    # read image names from input directory
    images = []
    images.extend(common.extract_file_names_from_dir(inputdir))
    # take only allowed extensions e.g. JPG
    images = filter(None,
                         [image_file if image_file.endswith(tuple(ALLOWED_EXTENSIONS)) else None for image_file in
                          images])
    return images


def match_collection(inputdir, query, params, summary):

    print 'match query:', query

    try:
        html_output_file = inputdir + "\\" + query.replace(".jpg", ".html")

        # generate matching pairs file
        if not exists_file(html_output_file):
            score_dict = search_similar_in_collection(inputdir, query, params, summary)
            generate_html_view(inputdir, inputdir, score_dict)
    except Exception as ex:
        print "Error by collection matching for query:", query, ex


def search_similar_in_collection(inputdir, query, params, summary):

    start = time.time()

    images = get_allowed_image_file_names_from_dir(inputdir)

    images = [query + " " + image for image in images]

    matching_output_file = inputdir + "\\" + query.replace(".", "-") + "-" + MATCH_OUTPUT_FILE

    # generate matching pairs file
    if not exists_file(matching_output_file):
        common.write_txt_file_from_list(inputdir, MATCHING_PAIRS_FILE, images)

        # generate non matching pairs file
        common.write_txt_file_from_list(inputdir, NON_MATCHING_PAIRS_FILE, [])

        exe = CDVS_BIN_FOLDER + "\\" + MATCH + "\\"  + MATCH + ".exe"
        params_path = inputdir + "\\" + params
        param_list = [exe, MATCHING_PAIRS_FILE, NON_MATCHING_PAIRS_FILE, MODE, inputdir, inputdir, "-t",
                      matching_output_file, "-o", "-m", "1", "-p", params_path]
        execute_command_using_cmd(param_list)

    score_dict = extract_score_from_match_output_file(inputdir, matching_output_file, images, summary)
    map(lambda x: x.append(inputdir), score_dict)

    end = time.time()
    print 'Matching calculation time :', end - start, ' for query:', query

    return score_dict


def cleanup(inputdir, dirnames):

    # clean up directories
    pass
    print '+++ CDVS cleanup completed +++'


def read_demo_summary(inputfile):

    print 'Read demo summary: ', inputfile
    with open(inputfile, 'rb') as csvfile:
        summary_reader = csv.reader(csvfile, delimiter=';', quotechar='|')
        summary = []
        for row in summary_reader:
            summary.append(row)
        return summary


def add_filename_column(summary, outputfile):

    print("Add file name column converted from Europeana IDs in", outputfile)

    fieldnames = ['path',
                  'europeana_id',
                  'title',
                  'uri',
                  'filename']

    with open(outputfile, 'w') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames, delimiter=';', lineterminator='\n')

        for row in summary:
            europeana_id = row[EUROPEANA_ID_POS]
            file_name = europeana_id[1:].replace('/', '_') + JPG_EXT
            entry = {
                'path': row[PATH_POS],
                'europeana_id': row[EUROPEANA_ID_POS],
                'title': row[TITLE_POS],
                'uri': row[URI_POS],
                'filename': file_name
            }

            writer.writerow(entry)


def move_files(inputdir, summary):

    for row in summary:

        try:
            path = row[PATH_POS]
            if "f:" in path:
                path = path.replace("f:", "E:")
            file_name = row[FILENAME_POS]

            #short_file_name = file_name.split('_')[FILENAME_POS_IN_EUROPEANA_NAME]
            new_path = inputdir + "\\" + file_name #path.replace(short_file_name, file_name)
            print("Move file", file_name)
            os.rename(path, new_path)
        except Exception as ex:
            print "Error by moving file name:", file_name, ex


# Main analyzing routine

def analyze_images(inputdir, use_case, query, mode, params):

    start = time.time()
    print("Analysing '" + inputdir + "' images...")
    print("Use case:", use_case, "query:", query)

    if use_case == ALL_TEST:
        all_test(inputdir, mode, params)

    if use_case == EXTRACT_MATCH_COLLECTION:
        extract_match_collection(inputdir, query, params)

    if use_case == EXTRACT:
        extract(inputdir)

    if use_case == INDEX:
        index(inputdir)

    if use_case == RETRIEVE:
        retrieve(inputdir)

    if use_case == MATCH:
        match(inputdir)

    if use_case == SEARCH_COLLECTION:
        search_collection(inputdir, query, params)

    if use_case == GENERATE_FILENAMES:
        generate_filenames(inputdir)

    if use_case == EUROPEANA_RENAME:
        europeana_rename(inputdir)

    end = time.time()
    print 'Calculation time:', end - start


# Command line parsing

if __name__ == '__main__':

    parser = argparse.ArgumentParser(
                    description="Finding similar Europeana images using CDVS library.")
    parser.add_argument('inputdir', type=str, default="E:\\app-test\\europeana-client\\image", help="Input image files to be processed")
    parser.add_argument('-u', '--use_case', type=str, nargs='?',
                        help="Analysis use cases in given order, such as 'all-test', 'extract-match-collection', 'extract', 'match', 'index', 'retrieve', 'search-collection', 'generate-filenames', 'europeana-rename', 'cleanup'")
    parser.add_argument('-q', '--query', type=str, default="", help="Query image file name")
    parser.add_argument('-m', '--mode', type=str, default="", help="Mode of the command. e.g. F for CDVS feature extraction")
    parser.add_argument('-p', '--params', type=str, default="", help="File name of the customized CDVS parameters that should be in root folder")

    if len(sys.argv) < 2:
        parser.print_help()
        sys.exit(1)

    args = parser.parse_args()
    analyze_images(args.inputdir, args.use_case, args.query, args.mode, args.params)
