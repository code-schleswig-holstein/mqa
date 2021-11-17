#!/usr/bin/env python
# coding: utf-8

# MIT License
# 
# Copyright (c) 2021 Dr. Jesper Zedlitz <jesper.zedlitz@melund.landsh.de>
# 
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
# 
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
# 
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import urllib.parse
import requests
import pandas as pd
from io import StringIO

SPARQL_ENDPOINT = "https://www.govdata.de/sparql"

SPARQL_PREFIXES = """PREFIX dc: <http://purl.org/dc/elements/1.1/>
PREFIX dct: <http://purl.org/dc/terms/>
PREFIX dcatde: <http://dcat-ap.de/def/dcatde/>
PREFIX dcat: <http://www.w3.org/ns/dcat#>
PREFIX foaf: <http://xmlns.com/foaf/0.1/>"""

def build_sparql_query_for_contributor( scope, result_name, filter_query ):
    query = SPARQL_PREFIXES
    query += "SELECT ?contributor (COUNT(?"+scope+") AS ?"+result_name+") WHERE {"
    if( "distribution" == scope):
        query += """?dataset a dcat:Dataset .
  ?dataset dcatde:contributorID ?contributor .
  FILTER( regex(str(?contributor), "^http://dcat-ap.de/def/contributors/" ) )
  ?dataset dcat:distribution ?distribution .
  ?distribution a dcat:Distribution ."""
    if( "dataset" == scope):
        query += """?dataset a dcat:Dataset .
  ?dataset dcatde:contributorID ?contributor .
  FILTER( regex(str(?contributor), "^http://dcat-ap.de/def/contributors/" ) )
  FILTER(NOT EXISTS { ?dataset dct:type <http://dcat-ap.de/def/datasetTypes/collection> })"""
    query += filter_query
    query += "}  GROUP BY ?contributor";
    return query

def query_for_contributors( scope, result_name, filter_query ):
    query = build_sparql_query_for_contributor(scope,result_name,filter_query)
    url = SPARQL_ENDPOINT + "?query=" + urllib.parse.quote(query)
    r = requests.get(url, headers={'Accept': 'text/csv'})
    data = StringIO(r.text)
    df = pd.read_csv(data)
    df['contributor'] = df['contributor'].apply(lambda s: s.replace('http://dcat-ap.de/def/contributors/', ''))
    df = df.groupby('contributor').sum()
    df.reset_index(inplace=True)
    return df

# Zähle Datensätze und Distributionen

all_distributions = query_for_contributors( "distribution", "total", "" )
all_datasets = query_for_contributors( "dataset","total","") 


# Zugänglichkeit - Download URL
df = query_for_contributors("distribution", 
                            "accessibility_download", 
                            "FILTER(NOT EXISTS { ?distribution dcat:downloadURL ?x. })")
all_distributions = all_distributions.merge(df,how='left')


# Wiederverwendbarkeit - Zugangsbeschränkungsangaben
df = query_for_contributors("dataset", 
                            "reusability_access_rights", 
                            "FILTER(NOT EXISTS { ?dataset dct:accessRights ?x. })")
all_datasets = all_datasets.merge(df,how='left')

# Wiederverwendbarkeit - Lizenzangaben
df = query_for_contributors("distribution", 
                            "reusability_license_information", 
                            "FILTER(NOT EXISTS { ?distribution dct:license ?x. })")
all_distributions = all_distributions.merge(df,how='left')

# Wiederverwendbarkeit - Zugangsbeschränkungsangaben aus Vokabular
df = query_for_contributors("dataset", 
                            "reusability_access_rights_vocabulary", 
                            "OPTIONAL { ?dataset dct:accessRights ?rights } . FILTER(  !regex(str(?rights), \"^http://publications.europa.eu/resource/authority/access-right/\" ) )")
all_datasets = all_datasets.merge(df,how='left')

# Wiederverwendbarkeit - Kontaktinformation
df = query_for_contributors("dataset", 
                            "reusability_contact", 
                            "FILTER(NOT EXISTS { ?dataset dcat:contactPoint ?x. })")
all_datasets = all_datasets.merge(df,how='left')

# Wiederverwendbarkeit - Herausgeber
df = query_for_contributors("dataset", 
                            "reusability_publisher", 
                            "FILTER(NOT EXISTS { ?dataset dct:publisher ?x. })")
all_datasets = all_datasets.merge(df,how='left')


# Kontext - Rechte
df = query_for_contributors("dataset", 
                            "contextuality_rights", 
                            "FILTER(NOT EXISTS { ?dataset dct:rights ?x. })")
all_datasets = all_datasets.merge(df,how='left')

# Kontext - Dataset: Änderungsdatum
df = query_for_contributors("dataset", 
                            "contextuality_dataset_modified", 
                            "FILTER(NOT EXISTS { ?dataset dct:modified ?x. })")
all_datasets = all_datasets.merge(df,how='left')

# Kontext - Dataset: Ausstellungsdatum
df = query_for_contributors("dataset", 
                            "contextuality_dataset_issued", 
                            "FILTER(NOT EXISTS { ?dataset dct:issued ?x. })")
all_datasets = all_datasets.merge(df,how='left')

# Kontext - Distribution: Änderungsdatum
df = query_for_contributors("distribution", 
                            "contextuality_distribution_modified", 
                            "FILTER(NOT EXISTS { ?distribution dct:modified ?x. })")
all_distributions = all_distributions.merge(df,how='left')

# Kontext - Distribution: Ausstellungsdatum
df = query_for_contributors("distribution", 
                            "contextuality_distribution_issued", 
                            "FILTER(NOT EXISTS { ?distribution dct:issued ?x. })")
all_distributions = all_distributions.merge(df,how='left')

# Kontext - Dateigröße
df = query_for_contributors("distribution", 
                            "contextuality_file_size", 
                            "FILTER(NOT EXISTS { ?distribution dcat:byteSize ?x. })")
all_distributions = all_distributions.merge(df,how='left')


# Auffindbarkeit - Schlüsselwörter
df = query_for_contributors("dataset", 
                            "findability_keyword", 
                            "FILTER(NOT EXISTS { ?dataset dcat:keyword ?x. })")
all_datasets = all_datasets.merge(df,how='left')

# Auffindbarkeit - Kategorien
df = query_for_contributors("dataset", 
                            "findability_category", 
                            "FILTER(NOT EXISTS { ?dataset dcat:theme ?x. })")
all_datasets = all_datasets.merge(df,how='left')

# Auffindbarkeit - Ortsbezogene Suche
df = query_for_contributors("dataset", 
                            "findability_geo", 
                            "FILTER(NOT EXISTS { ?dataset dct:spatial ?x. })")
all_datasets = all_datasets.merge(df,how='left')

# Auffindbarkeit - Zeitbasierte Suche
df = query_for_contributors("dataset", 
                            "findability_time", 
                            "FILTER(NOT EXISTS { ?dataset dct:temporal ?x. })")
all_datasets = all_datasets.merge(df,how='left')


# Interoperabilität - Format
df = query_for_contributors("distribution", 
                            "interoperability_format", 
                            "FILTER(NOT EXISTS { ?distribution dct:format ?x. })")
all_distributions = all_distributions.merge(df,how='left')

# Interoperabilität - Media Type
df = query_for_contributors("distribution", 
                            "interoperability_media_type", 
                            "FILTER(NOT EXISTS { ?distribution dcat:mediaType ?x. })")
all_distributions = all_distributions.merge(df,how='left')

# Interoperabilität - Format aus Vokabular
df = query_for_contributors("distribution", 
                            "interoperability_format_from_vocabulary", 
                            "OPTIONAL { ?distribution dct:format ?format }. FILTER(  !regex(str(?format), \"^http://publications.europa.eu/resource/authority/file-type/\"  ))")
all_distributions = all_distributions.merge(df,how='left')

# Interoperabilität - Media Type aus Vokabular
df = query_for_contributors("distribution", 
                            "interoperability_media_type_from_vocabulary", 
                            "OPTIONAL { ?distribution dct:format ?format } . FILTER(  !regex(str(?format), \"^https://www.iana.org/assignments/media-types/\"  ))")
all_distributions = all_distributions.merge(df,how='left')


# Sonstiges - zwei unterschiedliche Lizenzen an einer Distribution
df = query_for_contributors("distribution", 
                            "other_2licenses", 
                            "?distribution dct:license ?a . ?distribution dct:license ?b . FILTER( ?a != ?b )")
all_distributions = all_distributions.merge(df,how='left')


all_distributions = all_distributions.fillna(0)
all_datasets = all_datasets.fillna(0)

all_distributions.to_csv('distributions.csv')
all_datasets.to_csv('datasets.csv')

relative = pd.DataFrame( all_datasets['contributor'])
for c in all_distributions.columns.drop(['contributor','total']):
    relative[c] = 1-all_distributions[c].div(all_distributions['total'], axis=0)

relative_datasets = all_datasets[all_datasets.columns.drop(['total'])]
for c in all_datasets.columns.drop(['contributor','total']):
    relative[c] = 1-all_datasets[c].div(all_datasets['total'], axis=0)


relative.set_index(['contributor'], inplace=True)

relative.to_csv('relative.csv')
