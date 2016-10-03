##
##    In this module we test different methods for scoregraph project e.g. to wikidata helper.
##

import unittest
import wikidata_helper as wh

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
    # https://ia802302.us.archive.org/5/items/plf2014-07-30/plf2014-07-30_meta.xml
    @unittest.skip("demonstrating skipping")
    def test_extract_geo_location_from_ia_event(self):
        html_code = wh.get_html(TEST_IA_EVENT)
        venue = wh.extract_tag_from_html(html_code, TAG_VENUE)
        coverage = wh.extract_tag_from_html(html_code, TAG_COVERAGE)
        self.assertTrue(venue != None)
        self.assertTrue(coverage != None)


    # Extract geo location from Google starting from IA event like
    # https://ia802302.us.archive.org/5/items/plf2014-07-30/plf2014-07-30_meta.xml
    @unittest.skip("demonstrating skipping")
    def test_extract_geo_location_from_google_by_ia_event(self):
        html_code = wh.get_html(TEST_IA_EVENT)
        venue = wh.extract_tag_from_html(html_code, TAG_VENUE)
        google_html_response = wh.query_google(venue)
        docTuples = wh.extract_google_location_from_html(GOOGLE_LOCATION_PATTERN, google_html_response)
        google_location_url, google_location_label, street, city, country = wh.parseGoogleAddress(docTuples)
        self.assertTrue(venue != None)
        self.assertTrue(google_location_url != None)
        self.assertEqual(google_location_label, TEST_GOOGLE_LOCATION_TITLE)


    # use google location URL e.g. https://www.google.at/search?q=The+O2
    @unittest.skip("demonstrating skipping")
    def test_find_link_to_google_maps_from_address_label_using_google(self):
        searchQuery = wh.createGoogleSearchQuery(TEST_GOOGLE_LOCATION_TITLE)
        google_html_response = wh.get_html(searchQuery)
        docTuples = wh.extract_google_location_from_html(GOOGLE_MAPS_PATTERN, google_html_response)
        google_maps_url = wh.parseGoogleMapsStr(docTuples)
        print google_maps_url
        self.assertTrue(len(google_maps_url) > len(wh.GOOGLE_MAPS_BASE_PATH))


    def test_geolocation_using_geopy_and_nominatum(self):
        location_label = wh.geopy_geolocate_by_address_using_nominatum("The O2")
        location = wh.geopy_geolocate_by_address_using_nominatum("Peninsula Square, London SE10 0DX")
        address = wh.geopy_get_address_by_coordinates_using_nominatum("51.5024818,0.0050801")
        self.assertTrue(location_label != None)
        self.assertTrue(location != None)
        self.assertTrue(address != None)


if __name__ == '__main__':
    unittest.main()
    
