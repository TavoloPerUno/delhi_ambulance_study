import osmnx as ox
import requests
import time
import pandas as pd
import os
import googlemaps
from shapely.geometry import Point
from shapely.geometry.polygon import Polygon
from descartes import PolygonPatch
import networkx as nx
import pandana as pdna
import geopandas as gpd
import shutil
import ast
import math
from delhi_ambulance_study import util
import numpy as np



gmaps = googlemaps.Client(key='AIzaSyCC6DUYR8zCoWDX_RCaEXCcz3ZmXgS5X38')



def get_street_network(query_dict, output_folder, area_name):
    G = ox.graph_from_place(query_dict, network_type='drive_service')
    G_projected = ox.project_graph(G)

    ox.save_graph_shapefile(G_projected, filename=os.path.join(output_folder, area_name))
    try:
        shutil.rmtree(os.path.join(output_folder, area_name))
    except OSError:
        pass
    shutil.move(os.path.join('data', area_name), os.path.join(output_folder))
    shutil.rmtree('data')
    ox.save_graphml(G, filename=area_name + '.graphml', folder=os.path.join(output_folder, area_name))
    return G

def generate_speed_weights(gdf_edges, output_folder):
    gdf_edges_orig = gdf_edges.copy()
    gdf_edges['highway'] = list(map(str, gdf_edges['highway']))
    gdf_edges['maxspeed'] = list(map(util.tidy_maxspeed_tuple_to_int, gdf_edges['maxspeed']))
    dct_min_speed_by_category = dict(
        gdf_edges.loc[gdf_edges['maxspeed'].notnull()].groupby('highway')['maxspeed'].min())

    for idx, row in gdf_edges.loc[gdf_edges['maxspeed'].isnull()].iterrows():
        maxspeed = dct_min_speed_by_category.get(row['highway'], np.nan)
        if math.isnan(maxspeed):
            try:
                combo_types = ast.literal_eval(row['highway'])
                maxspeed = min([speed for speed in
                                [dct_min_speed_by_category.get(highwaytype, np.nan) for highwaytype in combo_types] if
                                not math.isnan(speed)])

            except:
                pass

        gdf_edges.loc[idx, 'maxspeed'] = maxspeed

    dct_min_speed_by_category['secondary_link'] = 35
    gdf_edges.loc[gdf_edges['highway'] == 'secondary_link', 'maxspeed'] = 35

    dct_min_speed_by_category['road'] = 15
    gdf_edges.loc[gdf_edges['highway'] == 'road', 'maxspeed'] = 15

    gdf_edges['time_to_traverse'] = gdf_edges['length'] / gdf_edges['maxspeed']

    gdf_edges_orig['time_to_traverse'] = gdf_edges['time_to_traverse']

    gdf_edges = gdf_edges_orig.copy()

    ox.save_load.save_gdf_shapefile(gdf_edges, filename="edges_edited", folder=os.path.join(output_folder, 'ncr'))

    return gdf_edges_orig

def graph_to_pandananet(gdf_edges,
                        gdf_nodes
                        ):

    twoway = list(~gdf_edges['oneway'].values)

    pdnet = pdna.Network(gdf_nodes['x'],
                         gdf_nodes['y'],
                         gdf_edges['u'],
                         gdf_edges['v'],
                         gdf_edges[["time_to_traverse"]],
                         twoway=twoway
                         )

    return pdnet

def driving_distance(area_graph, startpoint, endpoint):
    """
    Calculates the driving distance along an osmnx street network between two coordinate-points.
    The Driving distance is calculated from the closest nodes to the coordinate points.
    This can lead to problems if the coordinates fall outside the area encompassed by the network.

    Arguments:
    area_graph -- An osmnx street network
    startpoint -- The Starting point as coordinate Tuple
    endpoint -- The Ending point as coordinate Tuple
    """

    # Find nodes closest to the specified Coordinates
    node_start = ox.utils.get_nearest_node(area_graph, startpoint)
    node_stop = ox.utils.get_nearest_node(area_graph, endpoint)

    # Calculate the shortest network distance between the nodes via the edges "length" attribute
    distance = nx.shortest_path_length(area_graph, node_start, node_stop, weight="length")

    return distance

def geocode(address, api_key=None, return_full_response=False):
    """
    Get geocode results from Google Maps Geocoding API.

    Note, that in the case of multiple google geocode reuslts, this function returns details of the FIRST result.

    @param address: String address as accurate as possible. For Example "18 Grafton Street, Dublin, Ireland"
    @param api_key: String API key if present from google.
                    If supplied, requests will use your allowance from the Google API. If not, you
                    will be limited to the free usage of 2500 requests per day.
    @param return_full_response: Boolean to indicate if you'd like to return the full response from google. This
                    is useful if you'd like additional location details for storage or parsing later.
    """
    # Set up your Geocoding url
    geocode_url = "https://maps.googleapis.com/maps/api/geocode/json?address={}".format(address)
    if api_key is not None:
        geocode_url = geocode_url + "&key={}".format(api_key)

    # Ping google for the reuslts:
    results = requests.get(geocode_url)
    # Results will be in JSON format - convert to dict using requests functionality
    results = results.json()

    # if there's no results or an error, return empty results.
    if len(results['results']) == 0:
        output = {
            "formatted_address": None,
            "latitude": None,
            "longitude": None,
            "accuracy": None,
            "google_place_id": None,
            "type": None,
            "postcode": None
        }
    else:
        answer = results['results'][0]
        output = {
            "formatted_address": answer.get('formatted_address'),
            "latitude": answer.get('geometry').get('location').get('lat'),
            "longitude": answer.get('geometry').get('location').get('lng'),
            "accuracy": answer.get('geometry').get('location_type'),
            "google_place_id": answer.get("place_id"),
            "type": ",".join(answer.get('types')),
            "postcode": ",".join([x['long_name'] for x in answer.get('address_components')
                                  if 'postal_code' in x.get('types')])
        }

    # Append some other details:
    output['input_string'] = address
    output['number_of_results'] = len(results['results'])
    output['status'] = results.get('status')
    if return_full_response is True:
        output['response'] = results

    return output

def batch_geocode(addresses, output_folder, output_filename, api_key, return_full_results=False, backoff_time = 30):
    output_filename = os.path.join(output_folder, output_filename)
    # Create a list to hold results
    results = []
    # Go through each address in turn
    for address in addresses:
        # While the address geocoding is not finished:
        geocoded = False
        while geocoded is not True:
            # Geocode the address with google
            try:
                geocode_result = geocode(address, api_key, return_full_response=return_full_results)
            except Exception as e:
                print("Major error with {}".format(address))
                print("Skipping!")
                geocoded = True

            # If we're over the API limit, backoff for a while and try again later.
            if geocode_result['status'] == 'OVER_QUERY_LIMIT':
                print("Hit Query Limit! Backing off for a bit.")
                time.sleep(backoff_time * 60)  # sleep for 30 minutes
                geocoded = False
            else:
                # If we're ok with API use, save the results
                # Note that the results might be empty / non-ok - log this
                if geocode_result['status'] != 'OK':
                    print("Error geocoding {}: {}".format(address, geocode_result['status']))
                print("Geocoded: {}: {}".format(address, geocode_result['status']))
                results.append(geocode_result)
                geocoded = True

    # Print status every 100 addresses
    if len(results) % 100 == 0:
        print("Completed {} of {} address".format(len(results), len(addresses)))

    # Every 500 addresses, save progress to file(in case of a failure so you have something!)
    if len(results) % 500 == 0:
        pd.DataFrame(results).to_csv("{}_bak".format(output_filename))

    return results