##
##    In this module we test different methods for scoregraph project e.g. to wikidata helper.
##

import unittest
import wikidata_helper as wh
import geo_location_helper as glh

TEST_LABEL = 'Phil Lesh and Friends'
TEST_WIKIDATA_ID = 'http://www.wikidata.org/entity/Q2085676'
TEST_IA_EVENT = 'https://ia802302.us.archive.org/5/items/plf2014-07-30/plf2014-07-30_meta.xml'
TAG_VENUE = 'venue'
TAG_COVERAGE = 'coverage'
TEST_GOOGLE_LOCATION_TITLE = 'The O2'

GOOGLE_LOCATION_PATTERN = r'Adresse:&nbsp;</span><span class="_tA">(.*?)</span>'
GOOGLE_MAPS_PATTERN = r'<a href="https://maps.google.at/maps?(.*?)"'


class TestQueryMethods(unittest.TestCase):

    @unittest.skip("demonstrating skipping")
    def test_search_wikidata_by_label(self):
        entities = wh.retrieve_wikidata_objects_by_label(TEST_LABEL)
        self.assertTrue(len(entities) > 0)

    @unittest.skip("demonstrating skipping")
    def test_search_wikidata_by_label_via_sparql(self):
        wikidata_id = wh.retrieve_wikidata_entry_by_label_using_sparql(TEST_LABEL)
        self.assertEqual(wikidata_id, TEST_WIKIDATA_ID)


    # Extract geo location information from IA event like
if __name__ == '__main__':
    unittest.main()
    
