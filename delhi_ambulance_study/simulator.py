import pandas as pd
import numpy as np
import os
import csv
import datetime
import pytz
from delhi_ambulance_study import  util, gis_util, google_api_util
import osmnx as ox
from shapely.geometry import Point
from shapely.geometry.polygon import Polygon
from descartes import PolygonPatch
import networkx as nx
import pandana as pdna
import geopandas as gpd
import multiprocessing as mproc
import pickle
import copy
import requests
import time
from datetime import timedelta
import googlemaps

DCTLST_PLACES = [ {'state':'National Capital Territory', 'country':'India'}
                # {'district': 'Meerut', 'state':'Uttar Pradesh', 'country':'India'},
                # {'district': 'Muzzafarnagar', 'state':'Uttar Pradesh', 'country':'India'},
                # {'district': 'Ghaziabad', 'state':'Uttar Pradesh', 'country':'India'},
                # {'district': 'Gautam Budh Nagar', 'state':'Uttar Pradesh', 'country':'India'},
                # {'district': 'Bulandshahr', 'state':'Uttar Pradesh', 'country':'India'},
                # {'district': 'Baghpat', 'state':'Uttar Pradesh', 'country':'India'},
                # {'district': 'Hapur', 'state':'Uttar Pradesh', 'country':'India'},
                # {'district': 'Shamli', 'state':'Uttar Pradesh', 'country':'India'},
                # {'district': 'Faridabad', 'state': 'Haryana', 'country': 'India'},
                # {'district': 'Gurugram', 'state': 'Haryana', 'country': 'India'},
                # {'district': 'Mahendragarh', 'state': 'Haryana', 'country': 'India'},
                # {'district': 'Bhiwani', 'state': 'Haryana', 'country': 'India'},
                # {'district': 'Charkhi Dhadri', 'state': 'Haryana', 'country': 'India'},
                # {'district': 'Nuh', 'state': 'Haryana', 'country': 'India'},
                # {'district': 'Rohtak', 'state': 'Haryana', 'country': 'India'},
                # {'district': 'Sonipat', 'state': 'Haryana', 'country': 'India'},
                # {'district': 'Rewari', 'state': 'Haryana', 'country': 'India'},
                # {'district': 'Jhajjar', 'state': 'Haryana', 'country': 'India'},
                # {'district': 'Panipat', 'state': 'Haryana', 'country': 'India'},
                # {'district': 'Palwal', 'state': 'Haryana', 'country': 'India'},
                # {'district': 'Jind', 'state': 'Haryana', 'country': 'India'},
                # {'district': 'Karnal', 'state': 'Haryana', 'country': 'India'},
                # {'district': 'Alwar', 'state': 'Rajasthan', 'country': 'India'},
                # {'district': 'Bharatpur', 'state': 'Rajasthan', 'country': 'India'}
                ]

DATA_FOLDER = os.path.join('..', 'data')

with open(os.path.join(DATA_FOLDER, 'keys.csv'), 'r') as f:
    KEYS = list(csv.reader(f, delimiter=','))[0]

##Load hospitals data,
# fetch NCR road network info,
# generate crash information,
# generate taxi logs
# When generate is False, load all this information from pre-existing files

def load_data(data_folder,
              dctlst_places=DCTLST_PLACES,
              ntaxi=10000,
              ncrash=2000,
              period_start='2016-01-01 00:00:00',
              period_end='2017-01-01 00:00:00',
              generate=True):

    ncr_network = ''
    gdf_ncr = ''
    df_crash_events = ''
    df_delhi_hospitals = pd.read_csv(os.path.join(DATA_FOLDER, 'delhi_hospitals_final_geocoded.csv'))
    df_taxi_log = ''
    pdnet_ncr = ''

    dct_key_stats = dict()
    if os.path.isfile(os.path.join(DATA_FOLDER, 'dct_key_stats.pickle')):
        with open(os.path.join(DATA_FOLDER, 'dct_key_stats.pickle'), 'rb') as handle:
            dct_key_stats = pickle.load(handle)

    if generate:

        ncr_network = gis_util.get_street_network(dctlst_places, DATA_FOLDER, 'ncr')
        ncr_network = ox.load_graphml(filename='ncr.graphml', folder=os.path.join(DATA_FOLDER, 'ncr'))

        gdf_ncr = ox.graph_to_gdfs(ncr_network, edges=False)
        gdf_ncr.to_csv(os.path.join(DATA_FOLDER, 'ncr', 'ncr.csv'))
        gdf_edges = ox.graph_to_gdfs(ncr_network, nodes=False)
        gdf_edges = gis_util.generate_speed_weights(gdf_edges, data_folder)

        pdnet_ncr = gis_util.graph_to_pandananet(gdf_edges, gdf_ncr)

        crash_loc_indices = np.random.choice(gdf_ncr.index, size=ncrash, replace=True)

        time_points = pd.date_range(start=period_start, end=period_end, freq='15min', tz='Asia/Kolkata')

        df_crash_events = pd.DataFrame({'event_id': range(1, (ncrash + 1)),
                                        'event_time': np.random.choice(time_points, size=ncrash, replace=True),
                                        'event_lat': gdf_ncr.loc[crash_loc_indices]['y'],
                                        'event_lng': gdf_ncr.loc[crash_loc_indices]['x']
                                        })

        df_crash_events['event_time'] = pd.to_datetime(df_crash_events['event_time']).dt.tz_localize('Asia/Kolkata')

        df_crash_events.to_csv(os.path.join(DATA_FOLDER, 'crashes.csv'), index=False)

        taxi_loc_indices = np.random.choice(gdf_ncr.index, size=ntaxi, replace=True)
        time_points = pd.date_range(start='2015-12-29 00:00:00', end='2015-12-31 23:59:00', freq='0.5min',
                                tz='Asia/Kolkata')
        df_taxi_log = pd.DataFrame({'log_id': range(1, ntaxi + 1),
                                    'taxi_id': range(1, ntaxi + 1),
                                    'log_time': np.random.choice(time_points, size=ntaxi, replace=True),
                                    'log_lat': gdf_ncr.loc[taxi_loc_indices]['y'],
                                    'log_lng': gdf_ncr.loc[taxi_loc_indices]['x']
                                    })
        df_taxi_log['log_time'] = pd.to_datetime(df_taxi_log['log_time']).dt.tz_localize('Asia/Kolkata')
        df_taxi_log['log_is_active'] = True

        df_taxi_log.to_csv(os.path.join(DATA_FOLDER, 'taxi_log.csv'), index=False)





        dct_data = dict({'df_taxi_log': df_taxi_log,
                         'df_crash_events' : df_crash_events,
                         'gdf_ncr': gdf_ncr,
                         'ncr_network': ncr_network,
                         'gdf_edges': gdf_edges,
                         'gdf_nodes': gdf_ncr
                         })

        with open(os.path.join(DATA_FOLDER, 'dct_data.pickle'), 'wb') as handle:
            pickle.dump(dct_data, handle, protocol=pickle.HIGHEST_PROTOCOL)





    else:

        with open(os.path.join(DATA_FOLDER, 'dct_data.pickle'), 'rb') as handle:
            dct_data = pickle.load(handle)

        df_crash_events = dct_data['df_crash_events']
        df_taxi_log = dct_data['df_taxi_log']
        gdf_ncr = dct_data['gdf_nodes']
        ncr_network = dct_data['ncr_network']
        gdf_edges = dct_data['gdf_edges']
        pdnet_ncr = gis_util.graph_to_pandananet(gdf_edges,
                                                 gdf_ncr
                                                 )

    return df_delhi_hospitals, df_taxi_log, df_crash_events, gdf_ncr, ncr_network, pdnet_ncr, dct_key_stats




def get_nearest_pois(dct_mgr,
                      df_crash,
                      osmnet,
                      pdnet,
                      poi_name,
                      impedence,
                      n_poi,
                      df_poi_log,
                      lst_poi_id
                      ):



    if df_crash.shape[0] > 0:


        for idx, row in df_crash.iterrows():
            orig_lat = row['event_lat']
            orig_lng = row['event_lng']
            crash_node = ox.utils.get_nearest_node(osmnet, (orig_lat, orig_lng))

            df_curr_poi = None

            if df_poi_log is not None:

                df_curr_poi = df_poi_log.loc[(df_poi_log['log_time'] <= row['event_time']) &
                                               (df_poi_log['log_is_active']),]. \
                                        sort_values(by='log_time',
                                                    ascending=False). \
                                        groupby('taxi_id'). \
                                        first(). \
                                        reset_index()
                pdnet.set_pois(poi_name,
                               1000000,
                               n_poi,
                                df_curr_poi['log_lng'],
                                df_curr_poi['log_lat']
                                )
            print("Fetching nearest " + poi_name + " for crash id " + str(row['event_id']))
            df_nearest_pois = pdnet.nearest_pois(1000000,
                                                 poi_name,
                                                 num_pois=n_poi,
                                                 imp_name=impedence,
                                                 include_poi_ids=True
                                                 )

            nearest_pois = df_nearest_pois.loc[crash_node,]

            print("Nearest nearest " + poi_name + " for crash id " + str(row['event_id']) + " are ")
            print(nearest_pois)

            lst_nearest_poi = [int(nearest_pois['poi' + str(i)]) for i in list(range(1, (n_poi + 1)))]
            print("Nearest nearest " + poi_name + " for crash id " + str(row['event_id']) + " are ")
            print(lst_nearest_poi)

            if df_curr_poi is not None:

                lst_nearest_poi = [int(df_curr_poi.loc[i, 'log_id']) for i in lst_nearest_poi]
                print("Nearest nearest " + poi_name + " for crash id " + str(row['event_id']) + " are ")
                print(lst_nearest_poi)


            dct_mgr[row['event_id']] = lst_nearest_poi

        return


def get_shortest_travel_time(dct_crash_poi_times,
                             dct_crash_pois,
                             dct_key_stats,
                             df_crash,
                             df_pois,
                             proc_keys,
                             api_name,
                             api_limit,
                             poi_lat_col,
                             poi_lng_col,
                             poi_id_col
                            ):

    curr_key = proc_keys[0]

    if df_crash.shape[0] > 0:
        for idx, row in df_crash.iterrows():
            orig_lat = row['event_lat']
            orig_lng = row['event_lng']



            best_guess_time = []
            pessimistic_time = []
            optimistic_time = []
            for poi in dct_crash_pois[row['event_id']]:

                curr_key = google_api_util.get_valid_key(proc_keys,
                                                         dct_key_stats,
                                                         curr_key,
                                                         api_name,
                                                         api_limit
                                                         )

                dest_lat = df_pois.loc[df_pois[poi_id_col] == poi, poi_lat_col].values[0]
                dest_lng = df_pois.loc[df_pois[poi_id_col] == poi, poi_lng_col].values[0]

                dep_time = util.get_next_weekday(row['event_time'], row['event_time'].weekday())



                print("Computing best time to get to crash " + str(row['event_id']) + " from poi " + str(poi))




                best_time = google_api_util.get_trip_duration(orig_lat,
                                                              orig_lng,
                                                              dest_lat,
                                                              dest_lng,
                                                              dep_time,
                                                              curr_key,
                                                              "best_guess"
                                                              )

                print("Best time to get to crash " + str(row['event_id']) + " from poi " + str(poi) + " is " + str(
                    best_time))

                best_guess_time.append(best_time)

                curr_key = google_api_util.get_valid_key(proc_keys,
                                                         dct_key_stats,
                                                         curr_key,
                                                         api_name,
                                                         api_limit
                                                         )

                print(
                    "Computing pessimistic time to get to crash " + str(row['event_id']) + " from poi " + str(poi))
                pess_time = google_api_util.get_trip_duration(orig_lat,
                                                              orig_lng,
                                                              dest_lat,
                                                              dest_lng,
                                                              dep_time,
                                                              curr_key,
                                                              "pessimistic"
                                                              )

                pessimistic_time.append(pess_time)

                print("Pessimistic time to get to crash " + str(row['event_id']) + " from poi " + str(
                    poi) + " is " + str(
                    pess_time))

                curr_key = google_api_util.get_valid_key(proc_keys,
                                                         dct_key_stats,
                                                         curr_key,
                                                         api_name,
                                                         api_limit
                                                         )

                print("Computing optimistic time to get to crash " + str(row['event_id']) + " from poi " + str(poi))
                opt_time = google_api_util.get_trip_duration(orig_lat,
                                                             orig_lng,
                                                             dest_lat,
                                                             dest_lng,
                                                             dep_time,
                                                             curr_key,
                                                             "optimistic"
                                                             )

                print("Optimistic time to get to crash " + str(row['event_id']) + " from poi " + str(
                    poi) + " is " + str(
                    opt_time))
                optimistic_time.append(opt_time)


            best_guess_time = min(best_guess_time)
            pessimistic_time = min(pessimistic_time)
            optimistic_time = min(optimistic_time)



            dct_crash_poi_times[row['event_id']] = [best_guess_time, pessimistic_time, optimistic_time]

    return


def simulate(dctlst_places=DCTLST_PLACES,
             max_ntaxi=100,
             min_ntaxi=10,
             ncores=24
             ):

    lst_key_set = util.chunkIt(KEYS, ncores)
    api_name = 'directions'
    api_limit = 1000

    dct_ntaxi_response = dict()

    df_delhi_hospitals, \
    df_taxi_log, \
    df_crash_events, \
    gdf_ncr, \
    ncr_network,\
    pdnet_ncr, \
    dct_old_key_stats = load_data(DATA_FOLDER,
                            dctlst_places=dctlst_places,
                            ntaxi=10000,
                            ncrash=100,
                            period_start='2016-01-01 00:00:00',
                            period_end='2017-01-01 00:00:00',
                            generate=False
                            )

    manager = mproc.Manager()
    dct_key_stats = manager.dict()
    dct_key_stats.update(dct_old_key_stats)

    n_hospital_poi = 2
    n_taxi_poi = 3
    hospital_poi_name = "hospitals"
    taxi_poi_name = "taxis"
    impedence = "time_to_traverse"

    pdnet_ncr.set_pois(hospital_poi_name,
                       1000000,
                       n_hospital_poi,
                       df_delhi_hospitals['lng'],
                       df_delhi_hospitals['lat']
                       )

    df_crash_events = df_crash_events.sort_values(by=['event_time'])

    lst_df_crash = np.array_split(df_crash_events, ncores)

    for i in range(ncores - 1):

        while True:
            if (lst_df_crash[i].iloc[-1]['event_time'] - lst_df_crash[i + 1].iloc[0]['event_time']).seconds / 60.0 <= 6:
                lst_df_crash[i] = lst_df_crash[i].append(lst_df_crash[i + 1].iloc[0])
                lst_df_crash[i + 1] = lst_df_crash[i + 1].iloc[1:]
                continue
            break

    hospital_jobs = []

    dct_crash_hospitals = manager.dict()

    for i in list(range(ncores)):
        df_crash = lst_df_crash[i]
        p = mproc.Process(target=get_nearest_pois,
                          args=(dct_crash_hospitals,
                                df_crash_events.loc[df_crash.index],
                                copy.copy(ncr_network),
                                copy.copy(pdnet_ncr),
                                hospital_poi_name,
                                impedence,
                                n_hospital_poi,
                                None,
                                []
                                )
                          )
        hospital_jobs.append(p)

    for proc in hospital_jobs:
        print("Starting hospital job " + str(hospital_jobs.index(proc) + 1))
        proc.start()

    for proc in hospital_jobs:
        proc.join()
        print("Leaving hospital job " + str(hospital_jobs.index(proc) + 1))

    dct_old_key_stats.update(dct_key_stats)

    hospital_time_jobs = []
    dct_crash_hospital_times = manager.dict()

    for i in list(range(ncores)):
        df_crash = lst_df_crash[i]
        p = mproc.Process(target=get_shortest_travel_time,
                          args=(dct_crash_hospital_times,
                                dct_crash_hospitals,
                                dct_key_stats,
                                df_crash_events.loc[df_crash.index],
                                df_delhi_hospitals,
                                lst_key_set[i],
                                api_name,
                                api_limit,
                                'lat',
                                'lng',
                                'hosp_id',
                                )
                          )
        p.start()
        hospital_time_jobs.append(p)
        print("Starting hospital time job " + str(hospital_time_jobs.index(p) + 1))


    for ht_job in hospital_time_jobs:
        ht_job.join()
        print("Leaving hospital time job " + str(hospital_time_jobs.index(ht_job) + 1))

    dct_old_key_stats.update(dct_key_stats)

    for n_taxi in range(min_ntaxi, (max_ntaxi + 1)):
        dct_crash_taxis = manager.dict()

        lst_taxi_id = np.random.choice(df_taxi_log['taxi_id'], size=n_taxi, replace=False)

        taxi_jobs = []
        for i in list(range(ncores)):
            df_crash = lst_df_crash[i]
            p = mproc.Process(target=get_nearest_pois,
                              args=(dct_crash_taxis,
                                    df_crash_events.loc[df_crash.index],
                                    copy.copy(ncr_network),
                                    copy.copy(pdnet_ncr),
                                    taxi_poi_name,
                                    impedence,
                                    n_taxi_poi,
                                    df_taxi_log.loc[df_taxi_log['taxi_id'].isin(lst_taxi_id)],
                                    lst_taxi_id,
                                    )
                              )
            taxi_jobs.append(p)

        for proc in taxi_jobs:
            print("Starting taxi job " + str(taxi_jobs.index(proc) + 1))
            proc.start()

        for proc in taxi_jobs:
            proc.join()
            print("Leaving taxi job " + str(taxi_jobs.index(proc) + 1))

        taxi_time_jobs = []
        dct_crash_taxi_times = manager.dict()

        for i in list(range(ncores)):
            df_crash = lst_df_crash[i]
            p = mproc.Process(target=get_shortest_travel_time,
                              args=(dct_crash_taxi_times,
                                    dct_crash_taxis,
                                    dct_key_stats,
                                    df_crash_events.loc[df_crash.index],
                                    df_taxi_log.loc[df_taxi_log['taxi_id'].isin(lst_taxi_id)],
                                    lst_key_set[i],
                                    api_name,
                                    api_limit,
                                    'log_lat',
                                    'log_lng',
                                    'log_id'
                                    )
                              )
            taxi_time_jobs.append(p)

        for proc in taxi_time_jobs:
            print("Starting taxi time job " + str(taxi_time_jobs.index(proc) + 1))
            proc.start()

        for proc in taxi_time_jobs:
            proc.join()
            print("Leaving taxi time job " + str(taxi_time_jobs.index(proc) + 1))

        dct_old_key_stats.update(dct_key_stats)

        dct_travel_time = dict()
        for i in list(dct_crash_hospital_times.keys()):
            dct_travel_time[i] = {'best_guess': dct_crash_hospital_times[i][0] + dct_crash_taxi_times[i][0],
                                  'optimistic': dct_crash_hospital_times[i][2] + dct_crash_taxi_times[i][2],
                                  'pessimistic': dct_crash_hospital_times[i][1] + dct_crash_taxi_times[i][1]

            }

        df_travel_time = pd.DataFrame(dct_travel_time).T

        dct_ntaxi_response[n_taxi] = {'best_guess' : df_travel_time['best_guess'].mean(),
                                      'optimistic': df_travel_time['optimistic'].mean(),
                                      'pessimistic': df_travel_time['pessimistic'].mean()}

    with open(os.path.join(DATA_FOLDER, 'dct_ntaxi_response.pickle'), 'wb') as handle:
        pickle.dump(dct_ntaxi_response, handle, protocol=pickle.HIGHEST_PROTOCOL)

    with open(os.path.join(DATA_FOLDER, 'dct_key_stats.pickle'), 'wb') as handle:
        pickle.dump(dct_old_key_stats, handle, protocol=pickle.HIGHEST_PROTOCOL)

    print(dct_ntaxi_response)

with open(os.path.join(DATA_FOLDER, 'dct_key_stats.pickle'), 'rb') as handle:
    dct_key_stats = pickle.load(handle)

simulate(dctlst_places=DCTLST_PLACES,
         max_ntaxi=100,
         min_ntaxi=99,
         ncores=8)

