# -*- coding: utf-8 -*-
"""
Module for generating zxy tiles
.. module:: tileGenerator.generator
   :platform: Unix, Windows
   :synopsis: Module for generating zxy tiles
"""

from os import stat
import requests
import json
import datetime

class Downloader():
    """
    
    """

    def __init__(self, url, token=None):
        """
        Init method

        :param min_zoom: The minimum zoom level to start generating tiles.
        :param max_zoom: The maximum zoom level to stop generating tiles.
        :param bounds: The bounding box to generate tiles from.
        :type min_zoom: int
        :type max_zoom: int
        :type bounds: list

        """

        self.url = url

        self.service_url = f"{url}?f=pjson"

        if token is not None:
            self.service_url += f"&token={token}"
        
        r = requests.get(self.service_url)

        data = r.json()

        self.max_number_of_features_per_query = data['maxRecordCount']

        self.max_number_of_features_per_query = 250

    def download(
        self,
        file_name:str,
        fields:str=None,
        filter:str="1=1"
    ):
        start_time = datetime.datetime.now()
        
        feature_stats_url = f"{self.url}/query?where={filter}&returnGeometry=false&returnIdsOnly=true&f=pjson"

        r = requests.get(feature_stats_url)

        data = r.json()

        object_ids = data['objectIds']

        number_of_features = len(data['objectIds'])

        print(f"Endpoint has {number_of_features} features.")

        if number_of_features <= self.max_number_of_features_per_query:
            print(f"Downloading all features.")
            if fields == None:
                fields = "*"
            r = requests.get(f"{self.url}/query?where={filter}&outFields={fields}&returnGeometry=true&geometryPrecision=4&outSR=4326&f=geojson")

            data = r.json()  
            
            with open(f'{file_name}.geojson', 'w') as json_file:
                json.dump(data, json_file)

        else:
            start = 0

            if fields == None:
                fields = "*"

            for x in range( start, number_of_features, self.max_number_of_features_per_query ):
                feature_collection = {
                    "type": "FeatureCollection",           
                    "features": []
                }
                print(f"Downloading features {x} thru {x + self.max_number_of_features_per_query}")
                ids_requested = object_ids[x: x + self.max_number_of_features_per_query ]
                payload = {
                    'f': 'geojson', 
                    'objectIds': str( ids_requested )[1:-1],
                    'outSR': '4326',
                    'returnGeometry': 'true',
                    'outFields': fields,
                    'geometryPrecision': '4'
                }
                result = requests.post( f"{self.url}/query", data=payload ) 
                
                if 'error' in result.json():
                    print(result.json()['error'])

                feature_collection['features'] += result.json().get('features') 

                if x == 0:

                    with open(f'{file_name}.geojson', 'w') as json_file:
                        data = json.dumps(feature_collection)
                        json_file.write(data[:-2])
                
                else:
                    with open(f'{file_name}.geojson', 'a') as json_file:
                        data = json.dumps(feature_collection['features'])
                        data = data[:-1]
                        data = data[1:]
                        json_file.write(",")
                        json_file.write(data)
            with open(f'{file_name}.geojson', 'a') as json_file:
                json_file.write("]}")
        
        total_time = (datetime.datetime.now() - start_time).total_seconds()

        print(f"It took {int(total_time)} seconds to download your data.")