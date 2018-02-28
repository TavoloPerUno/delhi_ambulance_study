from datetime import datetime, timedelta
import pause
import googlemaps
import json, urllib
from urllib.parse import urlencode
from urllib.request import urlopen
import time
import requests
import asyncio

import aiohttp


async def get_http(url):
    async with aiohttp.ClientSession(conn_timeout=10, read_timeout=15) as client:
        try:
            async with client.get(url) as response:
                content = await response.text()
                return content, response.status
        except Exception:
            pass

def get_valid_key(keys, dct_key_usage, curr_idx, api_name, api_limit):

    start_key = curr_idx
    while True:
        if curr_idx not in dct_key_usage or \
                (dct_key_usage[curr_idx][api_name]['calls'] <= (api_limit - 1) and ((dct_key_usage[curr_idx][api_name]['last_call'] - datetime.now()).seconds/3600 < 24)):
            dct_key_usage[curr_idx] = {api_name: {'calls': (dct_key_usage[curr_idx][api_name]['calls'] if curr_idx in dct_key_usage else 0) + 1,
                                                  'last_call': datetime.now()}}
            return curr_idx

        if (dct_key_usage[curr_idx][api_name]['last_call'] - datetime.now()).seconds/3600 > 24:
            dct_key_usage[curr_idx] = {api_name: {'calls': 1,
                                                  'last_call': datetime.now()}}
            return curr_idx

        curr_idx = keys[(keys.index(curr_idx) + 1) % len(keys) ]

        if start_key == curr_idx:
            lst_last_call_times = [dct_key_usage[idx][api_name]['last_call'] for idx in range(0, len(keys))]
            time_to_resume = min(lst_last_call_times) + timedelta(hours=24)

            pause.until(time_to_resume)

            dct_key_usage[lst_last_call_times.index(min(lst_last_call_times))] = {api_name: {'calls': 1,
                                                                                            'last_call': datetime.now()}}
            return curr_idx

def get_trip_duration(lat1,
                      lng1,
                      lat2,
                      lng2,
                      dep_time,
                      key,
                      traffic_model="best_guess", timeout=5):

    try:


        url = 'https://maps.googleapis.com/maps/api/directions/json?%s' % urlencode((
                ('origin', str(lat1) + ',' + str(lng1)),
                ('destination', str(lat2) + ',' + str(lng2)),
                ('key', key),
                ('mode', 'driving'),
                ('alternatives', 'False'),
                ('departure_time', str(int(time.mktime(dep_time.timetuple())))),
                ('units', 'metric'),
                ('optimize_waypoints', 'False'),
                ('traffic_model', traffic_model)
        ))

        print(url)

        loop = asyncio.get_event_loop()
        task = loop.create_task(get_http(url))
        loop.run_until_complete(task)
        result = task.result()
        if result is not None:
            content, status = task.result()
            content = json.loads(content)
            if status == 200:
                travel_time = 0
                for lg in content['routes'][0]['legs']:
                    travel_time += lg['duration']['value']

                return (travel_time)
        else:
            return (99999999999999999999999999999999999999999999)
    except Exception as ex:
        print(ex)
        return (99999999999999999999999999999999999999999999)
