#!/usr/bin/env python
"""
Script for summarizing data for statistics generation.

Invocation:
$ python wikidata_helper.py data/normalized/*.json -o summary_authors.csv
"""

# HTTP connection
import requests

#!/usr/bin/python
import ConfigParser
config = ConfigParser.RawConfigParser()
config.read('geo.properties')
GEONAMES_USER_NAME = config.get('GeonamesSection', 'geonames.username');
GEONAMES_SEARCH_URI_REST_API = 'http://api.geonames.org/searchJSON?'
GEONAMES_SEARCH_REVERSE_GEOCODING_URI_REST_API = 'http://api.geonames.org/findNearbyPlaceNameJSON?'
GEONAMES_SEARCH_REVERSE_GEOCODING_EXTENDED_FIND_NEARBY_URI_REST_API = 'http://api.geonames.org/extendedFindNearby?'
GEONAMES_SEARCH_REVERSE_GEOCODING_FIND_NEARBY_WIKIPEDIA = 'http://api.geonames.org/findNearbyWikipediaJSON?'

GOOGLE_QUERY_BASE_URL = 'https://www.google.at/search?q='
GOOGLE_MAPS_BASE_PATH = 'https://maps.google.at/maps/'


# HTTP request for HTML content for particular URL
def get_html(url):
    r = requests.get(url)
    print 'status code', r.status_code
    if r.status_code != 200:
        print("FAILURE: Request", r.url, "failed")
        return None
    return r.content


def createGoogleSearchQuery(queryText):
    return GOOGLE_QUERY_BASE_URL + queryText.replace(' ', '+')


# HTTP request for HTML content for particular query from Google
def query_google(query):
    query_url = createGoogleSearchQuery(query)
    print 'query url:', query_url
    return get_html(query_url)


# this method extracts passed tag value from a given HTML source
def extract_tag_from_html(html_source, tag_name):
    try:
        from BeautifulSoup import BeautifulSoup
    except ImportError:
        from bs4 import BeautifulSoup
    parsed_html = BeautifulSoup(html_source)
    value = parsed_html.find(tag_name).text
    print tag_name, ":", value
    return value


# this method extracts google location URL and title from a given HTML source
def extract_google_location_from_html(pattern, html_source):
    import re
    docsPattern = pattern
    docTuples = re.findall(docsPattern, html_source, re.DOTALL)
    print docTuples
    return docTuples


# this method parses Google map URL
def parseGoogleMapsStr(docTuples):
    location = docTuples[0].replace('&amp;','&')
    return GOOGLE_MAPS_BASE_PATH + location


# this method parses Google address into parts to facilitate further search
def parseGoogleAddress(docTuples):
    LABEL_POS = 0
    STREET_POS = 1
    CITY_POS = 2
    COUNTRY_POS = 3
    address = docTuples[0].split(",")
    return createGoogleSearchQuery(address[LABEL_POS]), address[LABEL_POS], address[STREET_POS], address[CITY_POS], address[COUNTRY_POS]


def geopy_geolocate_by_address_using_nominatum(query):
    from geopy.geocoders import Nominatim
    geolocator = Nominatim()
    location = geolocator.geocode(query)
    print 'address: ', location.address
    print 'coordinates:', location.latitude, location.longitude
    print 'location raw:', location.raw
    return location


# find address by coordinates string that contains latitude and longitude
def geopy_get_address_by_coordinates_using_nominatum(coordinates):
    from geopy.geocoders import Nominatim
    geolocator = Nominatim()
    address = geolocator.reverse(coordinates)
    print 'address: ', address.address
    print 'coordinates:', address.latitude, address.longitude
    print 'location raw:', address.raw
    return address


def geopy_geolocate_by_address_using_geonames(query):
    from geopy.geocoders import GeoNames
    print 'Search by Geonames location, query: ', query
    geolocator = GeoNames(username=GEONAMES_USER_NAME)
    location = geolocator.geocode(query)
    if location != None:
        print 'Search by Geonames location, address: ', location.address
        print 'Search by Geonames location, coordinates:', location.latitude, location.longitude
        print 'Search by Geonames location, location raw:', location.raw
    else:
        print 'Search by Geonames location, location for query:', query, 'could not be found.'
    return location


# find address by coordinates string that contains latitude and longitude
def geopy_get_address_by_coordinates_using_geonames(coordinates):
    from geopy.geocoders import GeoNames
    print 'Search by Geonames coordinates, query: ', coordinates
    geolocator = GeoNames(username=GEONAMES_USER_NAME)
    address = geolocator.reverse(coordinates)
    if address != None:
        print 'Search by Geonames coordinates, address: ', address[0].address
        print 'Search by Geonames coordinates, coordinates:', address[0].latitude, address[0].longitude
        print 'Search by Geonames coordinates, location raw:', address[0].raw
    else:
        print 'Search by Geonames coordinates, address for coordinates:', coordinates, 'could not be found.'
    return address


import urllib
import json

# This method retruns JSON response for Geonames query by a place name from
# http://www.geonames.org/export/geonames-search.html
# e.g. http://api.geonames.org/searchJSON?q=london&username=demo
def geopy_get_coordinates_by_place_name(place_name):
    search_uri = GEONAMES_SEARCH_URI_REST_API
    search_uri += 'q='
    # encode query in utf-8
    search_uri += urllib.quote(place_name.encode('utf-8'))
    search_uri += '&username='
    search_uri += GEONAMES_USER_NAME
    response = get_html(search_uri)
    return response


# This method returns JSON response for Geonames query by coordinates (reverse geocoding) from
# http://www.geonames.org/export/reverse-geocoding.html
# e.g. http://api.geonames.org/findNearbyPlaceNameJSON?lat=47.3&lng=9&username=demo
def geopy_get_place_by_coordinates(lat, lng):
    search_uri = GEONAMES_SEARCH_REVERSE_GEOCODING_URI_REST_API
    search_uri += 'lat='
    search_uri += str(lat)
    search_uri += '&lng='
    search_uri += str(lng)
    search_uri += '&username='
    search_uri += GEONAMES_USER_NAME
    #search_uri += '&radius=5'
    response = get_html(search_uri)
    return response


# This method returns JSON response for Geonames query by coordinates (reverse geocoding) from
# http://www.geonames.org/export/reverse-geocoding.html
# e.g. http://api.geonames.org/extendedFindNearby?lat=47.3&lng=9&username=demo
def geopy_get_place_by_coordinates_extended_find_nearby(lat, lng):
    search_uri = GEONAMES_SEARCH_REVERSE_GEOCODING_EXTENDED_FIND_NEARBY_URI_REST_API
    search_uri += 'lat='
    search_uri += str(lat)
    search_uri += '&lng='
    search_uri += str(lng)
    search_uri += '&username='
    search_uri += GEONAMES_USER_NAME
    response = get_html(search_uri)
    return response


# This method returns JSON response for Geonames query by coordinates (reverse geocoding) from
# http://www.geonames.org/export/wikipedia-webservice.html#findNearbyWikipedia
# e.g. http://api.geonames.org/findNearbyWikipedia?lat=47.3&lng=9&username=demo
def geopy_get_place_by_coordinates_extended_find_nearby_wikipedia_articles(lat, lng):
    search_uri = GEONAMES_SEARCH_REVERSE_GEOCODING_FIND_NEARBY_WIKIPEDIA
    search_uri += 'lat='
    search_uri += str(lat)
    search_uri += '&lng='
    search_uri += str(lng)
    search_uri += '&username='
    search_uri += GEONAMES_USER_NAME
    response = get_html(search_uri)
    return response


# this method extracts particular Geonames fields from the HTTP response in JSON format
def parse_geonames_response(response):
    jsonRes = json.loads(response)['geonames'][0] # take the first result
    geonameId = jsonRes["geonameId"]
    toponymName = jsonRes["toponymName"]
    countryId = jsonRes["countryId"]
    countryCode = jsonRes["countryCode"]
    countryName = jsonRes["countryName"]
    name = jsonRes["name"]
    longitude = jsonRes["lng"]
    lattitude = jsonRes["lat"]
    return geonameId, toponymName, countryId, countryCode, countryName, name, longitude, lattitude


