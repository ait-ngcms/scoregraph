# scoregraph

This branch was created from the original sources: https://github.com/behas/scoregraph 

A collection of scripts for transforming ONB music score metadata into a semantically enriched knowledge graph of music scores.


## Data aggregation

Starting from the ONB's raw music score dataset (Aleph export) data aggregation involves the following steps:

+ Data normalization: extract relevant fields from raw aleph data ([example][ex_raw]) and transform to JSON ([example][ex_normalized])
+ Data enrichment ([example][ex_enriched]):
    + follow GND links in raw/normalized data and collect additional uris (e.g., DBpedia, VIAF)
    + find related Europeana items by (i) searching via the Europeana search API and (ii) filtering those that share at least one URI with the raw/normalized data
+ Statistics computation: ([example][summary_enriched])
    + id: aleph document id
    + links_artwork: number of artwork links
    + persons: number of persons mentioned in metadata record
    + links_person_gnd: number of persons linked to GND
    + links_person_dbpedia: number of persons linked to DBPedia
    + links_person_viaf: number of persons linked to VIAF
    + related_europeana_items: number of persons possibly related to Europeana

## Script usage

+ Preconditions: 
    + Python 2.7 
    + requests 2.7.0 package for HTTP requests
    + BeautifulSoup package for normalization
    + SPARQLWrapper 1.6.4 and rdflib 4.2.1 packages for SPARQL requests
    + goslate 1.5.0 package for Google translate requests

Install dependencies:

    pip install -r requirements.txt


Enable script execution

    chmod u+x *.py

The dataset in the XML format should be placed in folder 'data/raw'

+ Run the whole analysis workflow including:
    + normalization (GND number)
    + enrichment (from Europeana)
    + statistics calculation (author, subject, title)
    + Dexter API enrichment (retrieves DBPedia IDs)
    + DBPedia enrichment

Start workflow analysis
	
	$ python analyze.py data

Run data normalization script

    ./normalize -o data/normalized data/raw/*.xml


Run data enrichment script

    ./enrich -o data/enriched -e YOUR_EUROPEANA_API_KEY data/normalized/*.json


[ex_raw]: ./data/raw/AL00119186.xml
[ex_normalized]: ./data/normalized/AL00119186.json
[ex_enriched]: ./data/enriched/AL00119186.json

[summary_normalized]: ./summary_normalized.csv
[summary_enriched]: ./summary_enriched.csv
