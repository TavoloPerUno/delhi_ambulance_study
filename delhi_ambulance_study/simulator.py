import pandas as pd
import numpy as np
import os
import csv
import datetime
import pytz
from delhi_ambulance_study import  util, gis_util
import osmnx as ox
from shapely.geometry import Point
from shapely.geometry.polygon import Polygon
from descartes import PolygonPatch
import networkx as nx
import pandana as pdna
import geopandas as gpd
import multiprocessing as mproc
import pickle


DCTLST_PLACES = [ {'state':'National Capital Territory', 'country':'India'},
                {'district': 'Meerut', 'state':'Uttar Pradesh', 'country':'India'},
                {'district': 'Muzzafarnagar', 'state':'Uttar Pradesh', 'country':'India'},
                {'district': 'Ghaziabad', 'state':'Uttar Pradesh', 'country':'India'},
                {'district': 'Gautam Budh Nagar', 'state':'Uttar Pradesh', 'country':'India'},
                {'district': 'Bulandshahr', 'state':'Uttar Pradesh', 'country':'India'},
                {'district': 'Baghpat', 'state':'Uttar Pradesh', 'country':'India'},
                {'district': 'Hapur', 'state':'Uttar Pradesh', 'country':'India'},
                {'district': 'Shamli', 'state':'Uttar Pradesh', 'country':'India'},
                {'district': 'Faridabad', 'state': 'Haryana', 'country': 'India'},
                {'district': 'Gurugram', 'state': 'Haryana', 'country': 'India'},
                {'district': 'Mahendragarh', 'state': 'Haryana', 'country': 'India'},
                {'district': 'Bhiwani', 'state': 'Haryana', 'country': 'India'},
                {'district': 'Charkhi Dhadri', 'state': 'Haryana', 'country': 'India'},
                {'district': 'Nuh', 'state': 'Haryana', 'country': 'India'},
                {'district': 'Rohtak', 'state': 'Haryana', 'country': 'India'},
                {'district': 'Sonipat', 'state': 'Haryana', 'country': 'India'},
                {'district': 'Rewari', 'state': 'Haryana', 'country': 'India'},
                {'district': 'Jhajjar', 'state': 'Haryana', 'country': 'India'},
                {'district': 'Panipat', 'state': 'Haryana', 'country': 'India'},
                {'district': 'Palwal', 'state': 'Haryana', 'country': 'India'},
                {'district': 'Jind', 'state': 'Haryana', 'country': 'India'},
                {'district': 'Karnal', 'state': 'Haryana', 'country': 'India'},
                {'district': 'Alwar', 'state': 'Rajasthan', 'country': 'India'},
                {'district': 'Bharatpur', 'state': 'Rajasthan', 'country': 'India'}
                ]

DATA_FOLDER = os.path.join('..', 'data')

def load_data(data_folder, dctlst_places=DCTLST_PLACES, ntaxi=10000, ncrash=2000, period_start='2016-01-01 00:00:00', period_end='2017-01-01 00:00:00', generate=True):

    ncr_network = ''
    gdf_ncr = ''
    df_crash_events = ''
    df_delhi_hospitals = pd.read_csv(os.path.join(DATA_FOLDER, 'delhi_hospitals_final_geocoded.csv'))
    df_taxi_log = ''

    if generate:
        ncr_network = gis_util.get_street_network(dctlst_places, DATA_FOLDER, 'ncr')
        ncr_network = ox.load_graphml(filename='ncr.graphml', folder=os.path.join(DATA_FOLDER, 'ncr'))

        gdf_ncr = gpd.read_file(os.path.join(DATA_FOLDER, 'ncr', 'nodes', 'nodes.shp'))

        gdf_ncr.to_csv(os.path.join(DATA_FOLDER, 'ncr', 'ncr.csv'))

        crash_loc_indices = np.random.choice(gdf_ncr.index, size=ncrash, replace=True)

        time_points = pd.date_range(start=period_start, end=period_end, freq='15min', tz='Asia/Kolkata')

        df_crash_events = pd.DataFrame({'event_time': np.random.choice(time_points, size=ncrash, replace=True),
                                    'event_lat': gdf_ncr.iloc[crash_loc_indices]['lat'],
                                    'event_lng': gdf_ncr.iloc[crash_loc_indices]['lon']
                                    })

        df_crash_events['event_time'] = pd.to_datetime(df_crash_events['event_time']).dt.tz_localize('Asia/Kolkata')

        df_crash_events.to_csv(os.path.join(DATA_FOLDER, 'crashes.csv'), index=False)

        taxi_loc_indices = np.random.choice(gdf_ncr.index, size=ntaxi, replace=True)
        time_points = pd.date_range(start='2015-12-29 00:00:00', end='2015-12-31 23:59:00', freq='0.5min',
                                tz='Asia/Kolkata')
        df_taxi_log = pd.DataFrame({'log_id': range(1, ntaxi + 1),
                                    'taxi_id': range(1, ntaxi + 1),
                                    'log_time': np.random.choice(time_points, size=ntaxi, replace=True),
                                    'log_lat': gdf_ncr.iloc[taxi_loc_indices]['lat'],
                                    'log_lng': gdf_ncr.iloc[taxi_loc_indices]['lon']
                                    })
        df_taxi_log['log_time'] = pd.to_datetime(df_taxi_log['log_time']).dt.tz_localize('Asia/Kolkata')
        df_taxi_log['log_is_active'] = True

        df_taxi_log.to_csv(os.path.join(DATA_FOLDER, 'taxi_log.csv'), index=False)

    else:
        df_crash_events = pd.read_csv(os.path.join(DATA_FOLDER, 'crashes.csv'))
        df_taxi_log = pd.read_csv(os.path.join(DATA_FOLDER, 'taxi_log.csv'))


        df_taxi_log['log_time'] = pd.to_datetime(df_taxi_log['log_time']).dt.tz_localize('UTC').dt.tz_convert(
            'Asia/Kolkata')
        df_crash_events['event_time'] = pd.to_datetime(df_crash_events['event_time']).dt.tz_localize(
            'UTC').dt.tz_convert('Asia/Kolkata')

        ncr_network = ox.load_graphml(filename='ncr.graphml', folder=os.path.join(DATA_FOLDER, 'ncr'))

        gdf_ncr = gpd.read_file(os.path.join(DATA_FOLDER, 'ncr', 'nodes', 'nodes.shp'))


    return df_delhi_hospitals, df_taxi_log, df_crash_events, gdf_ncr, ncr_network

def get_nearest_taxis_and_hospitals(proc_num, dct_proc_data, df_crash, df_taxi_log, lst_taxi_id, osmnet, pdnet):

    lst_taxis = []
    lst_hospitals = []

    for idx, row in df_crash.iterrows():
        orig_lat = row['event_lat']
        orig_lng = row['event_lng']
        crash_node = ox.utils.get_nearest_node(osmnet, (orig_lat, orig_lng))

        df_curr_taxi = df_taxi_log.loc[(df_taxi_log['taxi_id'].isin(lst_taxi_id)) & (
            df_taxi_log['log_time'] <= row['event_time']) & (df_taxi_log['log_is_active'] == True),].sort_values(
            by='log_time', ascending=False).groupby(
            'taxi_id').first().reset_index()

        pdnet_sub_ncr = pdnet

        pdnet_sub_ncr.set_pois("taxis", 1000000, 3, df_curr_taxi['log_lng'], df_curr_taxi['log_lat'])

        df_nearest_taxis = pdnet_sub_ncr.nearest_pois(1000000, "taxis", num_pois=3, imp_name='length',
                                                      include_poi_ids=True)

        nearest_taxis = df_nearest_taxis.loc[crash_node,]

        lst_taxis.append(
            [df_curr_taxi.loc[nearest_taxis['poi1']]['log_id'], df_curr_taxi.loc[nearest_taxis['poi2']]['log_id'],
             df_curr_taxi.loc[nearest_taxis['poi3']]['log_id']])

        df_nearest_hospitals = pdnet_sub_ncr.nearest_pois(1000000, "hospitals", num_pois=2, imp_name='length',
                                                          include_poi_ids=True)

        nearest_hospitals = df_nearest_hospitals.loc[crash_node,]

        lst_hospitals.append([nearest_hospitals['poi1'], nearest_hospitals['poi2']])

    dct_proc_data[proc_num] = [lst_hospitals, lst_taxis]



def simulate(dctlst_places=DCTLST_PLACES, max_ntaxi=100, min_ntaxi=10, ncores=24):

    df_delhi_hospitals, df_taxi_log, df_crash_events, gdf_ncr, ncr_network = load_data(DATA_FOLDER, dctlst_places=dctlst_places, ntaxi=10000, ncrash=2000, period_start='2016-01-01 00:00:00',
              period_end='2017-01-01 00:00:00', generate=False)

    pdnet_ncr = gis_util.graph_to_pandananet(ncr_network)

    pdnet_ncr.set_pois("hospitals", 1000000, 3, df_delhi_hospitals['lng'], df_delhi_hospitals['lat'])

    dct_taxi_hospitals = {}

    for i in range(min_ntaxi, max_ntaxi + 1):
        lst_taxi_id = np.random.choice(df_taxi_log['taxi_id'].unique(), size=i, replace=False)

        lst_taxis = []
        lst_hospitals = []

        manager = mproc.Manager()
        dct_proc_data = manager.dict()

        jobs = []
        lst_df_crash = np.array_split(df_crash_events, ncores)
        for i in range(ncores):

            p = mproc.Process(target=get_nearest_taxis_and_hospitals, args=(i, dct_proc_data, lst_df_crash[i], df_taxi_log, lst_taxi_id, ncr_network, pdnet_ncr,))
            jobs.append(p)
            p.start()

        for proc in jobs:
            proc.join()

        for i in range(ncores):
            lst_hospitals.append(dct_proc_data[i][0])
            lst_taxis.append(dct_proc_data[i][1])

        dct_taxi_hospitals[i] = [lst_hospitals, lst_taxis]


        with open(os.path.join(DATA_FOLDER, 'dct_taxi_hospitals.pickle'), 'wb') as handle:
            pickle.dump(dct_taxi_hospitals, handle, protocol=pickle.HIGHEST_PROTOCOL)




simulate(dctlst_places=[{'state':'National Capital Territory', 'country':'India'}], max_ntaxi=100, min_ntaxi=10, ncores=8)