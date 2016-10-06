##
##    In this module we test different methods for scoregraph project e.g. to geo location helper.
##

import unittest
import geo_location_helper as glh
import json

TEST_GOOGLE_LOCATION_TITLE = 'The O2'

GOOGLE_LOCATION_PATTERN = r'Adresse:&nbsp;</span><span class="_tA">(.*?)</span>'
GOOGLE_MAPS_PATTERN = r'<a href="https://maps.google.at/maps?(.*?)"'


class TestQueryMethods(unittest.TestCase):

    # use google location URL e.g. https://www.google.at/search?q=The+O2
    @unittest.skip("demonstrating skipping")
    def test_find_link_to_google_maps_from_address_label_using_google(self):
        searchQuery = glh.createGoogleSearchQuery(TEST_GOOGLE_LOCATION_TITLE)
        google_html_response = glh.get_html(searchQuery)
        docTuples = glh.extract_google_location_from_html(GOOGLE_MAPS_PATTERN, google_html_response)
        google_maps_url = glh.parseGoogleMapsStr(docTuples)
        print google_maps_url
        self.assertTrue(len(google_maps_url) > len(glh.GOOGLE_MAPS_BASE_PATH))

    @unittest.skip("demonstrating skipping")
    def test_geolocation_using_geopy_and_nominatum(self):
        location_label = glh.geopy_geolocate_by_address_using_nominatum("The O2")
        location = glh.geopy_geolocate_by_address_using_nominatum("Peninsula Square, London SE10 0DX")
        address = glh.geopy_get_address_by_coordinates_using_nominatum("51.5024818,0.0050801")
        self.assertTrue(location_label != None)
        self.assertTrue(location != None)
        self.assertTrue(address != None)

    @unittest.skip("demonstrating skipping")
    def test_geolocation_using_geopy_and_geonames(self):
        location_label = glh.geopy_geolocate_by_address_using_geonames("The O2")
        location_street = glh.geopy_geolocate_by_address_using_geonames("Peninsula Square, London SE10 0DX")
        location_city = glh.geopy_geolocate_by_address_using_geonames("London SE10 0DX")
        address = glh.geopy_get_address_by_coordinates_using_geonames("51.50252,0.00313")
        self.assertTrue(location_label != None)
        self.assertTrue(location_street == None)
        self.assertTrue(location_city == None)
        self.assertTrue(address != None)

    @unittest.skip("demonstrating skipping")
    def test_geolocation_using_geonames_rest_api(self):
        geonames_response = glh.geopy_get_coordinates_by_place_name("The O2")
        self.assertTrue(geonames_response != None)
        if geonames_response is not None:
            print geonames_response
            geonameId, toponymName, countryId, countryCode, countryName, name, longitude, lattitude = glh.parse_geonames_response(
                geonames_response)
            print 'geonameId:', geonameId, ', toponymName:', toponymName, ', countryId:', countryId, ', countryCode:', countryCode, \
                ', countryName:', countryName, ', name:', name, ', longitude:', longitude, ', lattitude:', lattitude
            self.assertEqual(geonameId, 6621342)
            self.assertEqual(toponymName, 'The O2')
            self.assertEqual(countryId, '2635167')
            self.assertEqual(countryCode, 'GB')
            self.assertEqual(countryName, 'United Kingdom')
            self.assertEqual(name, 'The Dome')
            self.assertEqual(longitude, '0.00313')
            self.assertEqual(lattitude, '51.50252')

    @unittest.skip("demonstrating skipping")
    def test_reverse_geolocation_using_geonames_rest_api(self):
        geonames_response = glh.geopy_get_place_by_coordinates(51.50252, 0.00313)
        self.assertTrue(geonames_response != None)
        if geonames_response is not None:
            print geonames_response
            geonameId, toponymName, countryId, countryCode, countryName, name, longitude, lattitude = glh.parse_geonames_response(
                geonames_response)
            print 'geonameId:', geonameId, ', toponymName:', toponymName, ', countryId:', countryId, ', countryCode:', countryCode, \
                ', countryName:', countryName, ', name:', name, ', longitude:', longitude, ', lattitude:', lattitude
            self.assertEqual(geonameId, 6621342)
            self.assertEqual(toponymName, 'The O2')
            self.assertEqual(countryId, '2635167')
            self.assertEqual(countryCode, 'GB')
            self.assertEqual(countryName, 'United Kingdom')
            self.assertEqual(name, 'The Dome')
            self.assertEqual(longitude, '0.00313')
            self.assertEqual(lattitude, '51.50252')


    @unittest.skip("demonstrating skipping")
    def test_reverse_geolocation_using_geonames_rest_api_find_nearby_ext(self):
        geonames_response = glh.geopy_get_place_by_coordinates_extended_find_nearby(51.50252, 0.00313)
        self.assertTrue(geonames_response != None)
        if geonames_response is not None:
            print geonames_response


    def test_reverse_geolocation_using_geonames_rest_api_find_nearby_wikipedia_articles(self):
        geonames_response = glh.geopy_get_place_by_coordinates_extended_find_nearby_wikipedia_articles(51.50252, 0.00313)
        self.assertTrue(geonames_response != None)
        if geonames_response is not None:
            print 'response:', geonames_response
            jsonRes = json.loads(geonames_response)['geonames'][0]  # take the first result
            geonameId = jsonRes["geoNameId"]
            title = jsonRes["title"]
            countryCode = jsonRes["countryCode"]
            wikipediaUrl = jsonRes["wikipediaUrl"]
            summary = jsonRes["summary"]
            longitude = round(jsonRes["lng"], 3)
            lattitude = round(jsonRes["lat"], 3)

            print 'geonameId:', geonameId, ', title:', title, ', countryCode:', countryCode, \
                ', wikipediaUrl:', wikipediaUrl, ', longitude:', longitude, ', lattitude:', lattitude
            print 'summary:', summary,


            self.assertEqual(geonameId, 6621342)
            self.assertEqual(title, 'The O2')
            self.assertEqual(countryCode, 'GB')
            self.assertEqual(wikipediaUrl, 'en.wikipedia.org/wiki/The_O2')
            self.assertEqual(longitude, 0.003)
            self.assertEqual(lattitude, 51.503)
            self.assertTrue('The O2' in summary)


if __name__ == '__main__':
    unittest.main()
    
