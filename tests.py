##
##    In this module we test different methods for scoregraph project e.g. to wikidata helper.
##

import unittest
import wikidata_helper as wh

TEST_LABEL = 'Phil Lesh and Friends'
TEST_WIKIDATA_ID = 'http://www.wikidata.org/entity/Q2085676'


class TestQueryMethods(unittest.TestCase):

    @unittest.skip("demonstrating skipping")
    def test_search_wikidata_by_label(self):
        entities = wh.retrieve_wikidata_objects_by_label(TEST_LABEL)
        self.assertTrue(len(entities) > 0)


    def test_search_wikidata_by_label_via_sparql(self):
        wikidata_id = wh.retrieve_wikidata_entry_by_label_using_sparql(TEST_LABEL)
        self.assertEqual(wikidata_id, TEST_WIKIDATA_ID)


if __name__ == '__main__':
    unittest.main()
    
