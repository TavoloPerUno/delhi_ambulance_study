

```python
import os
import pandas as pd
import requests
import time
import sys
import numpy as np
import csv
import datetime
import osmnx as ox
from shapely.geometry import Point
from shapely.geometry.polygon import Polygon
from descartes import PolygonPatch
import networkx as nx
import pandana as pdna
import geopandas as gpd
import ast
import math
import multiprocessing as mproc
import copy
```


```python
module_path = os.path.abspath(os.path.join('..'))
if module_path not in sys.path:
    sys.path.append(module_path)
    
from delhi_ambulance_study import gis_util, util
```


```python
DATA_FOLDER = os.path.join('..', 'data')
with open(os.path.join(DATA_FOLDER, 'keys.csv'), 'r') as f:
    keys = csv.reader(f)
    keys = list(keys)[0]
    
API_KEY = keys[0]
%matplotlib inline
```

# Hospital Locations

Using RML, Indu Rao,Amarindu, AIIMS, Sushrut, Deen Dayal Upadhyaya, Safdarjung, Lal bahadur shastry, GTB as the list of hospitals in Delhi, **(There is no hospital by the name Amarindu)**


```python
df_delhi_hospitals = pd.DataFrame({'name': ['Ram Manohar Lohia Hospital',
                                            'North DMC Medical College & Hindu Rao Hospital',
                                            'AIIMS',
                                            'Sushrut Trauma Centre',
                                            'Deen Dayal Upadhyay Hospital',
                                            'Safdarjung Hospital',
                                            'Lal Bahadur Shastri Hospital',
                                            'GTB Hospital'
                                           ],
                                   'street': ['Baba Kharak Singh Marg',
                                              'DR. J.S. Kkaranwal Memorial Road',
                                              'Safdarjung Enclave, Aurobindo Marg, Ansari Nagar',
                                              'Ring Road, Behind I.P. College, Near Civil Lines Metro Station, Metcalf Road',
                                              'Clock Tower Chowk, Hari Enclave',
                                              'Safdarjung Campus',
                                              'Near Kalyanvas Colony, Mayur Vihar, Phase -II',
                                              'GTB Enclave'
                                              
                                             ],
                                   'neighbourhood': ['Connaught Place',
                                                   'Malka Ganj',
                                                   'Haus Khas',
                                                   'Civil Lines',
                                                   'Hari Nagar',
                                                   'Ansari Nagar West',
                                                   'Khichripur',
                                                   'Shahdara'],
                                   'city': ['New Delhi',
                                            'New Delhi',
                                            'New Delhi',
                                            'New Delhi',
                                            'New Delhi',
                                            'New Delhi',
                                            'New Delhi',
                                            'New Delhi'
                                           ],
                                   'state': ['Delhi',
                                             'Delhi',
                                             'Delhi',
                                             'Delhi',
                                             'Delhi',
                                             'Delhi',
                                             'Delhi',
                                             'Delhi'
                                            ],
                                   'pin': ['110001',
                                           '110007',
                                           '110029',
                                           '110054',
                                           '110064',
                                           '110029',
                                           '110091',
                                           '110095'
                                          ],
                                   'country': ['India',
                                               'India',
                                               'India',
                                               'India',
                                               'India',
                                               'India',
                                               'India',
                                               'India'
                                             ]
                                  })
```


```python
addresses = list((df_delhi_hospitals['name'] + ',' + df_delhi_hospitals['street'] + ',' + df_delhi_hospitals['neighbourhood'] + ',' + df_delhi_hospitals['city'] + ',' + df_delhi_hospitals['state'] + ' ' + df_delhi_hospitals['pin'] + "," + df_delhi_hospitals['country']))
```

Geocoding using Google geocoding API,


```python
results = gis_util.batch_geocode(addresses, DATA_FOLDER, 'delhi_hospitals_latlong.csv', API_KEY)
```

    Geocoded: Ram Manohar Lohia Hospital,Baba Kharak Singh Marg,Connaught Place,New Delhi,Delhi 110001,India: OK
    Geocoded: North DMC Medical College & Hindu Rao Hospital,DR. J.S. Kkaranwal Memorial Road,Malka Ganj,New Delhi,Delhi 110007,India: OK
    Geocoded: AIIMS,Safdarjung Enclave, Aurobindo Marg, Ansari Nagar,Haus Khas,New Delhi,Delhi 110029,India: OK
    Geocoded: Sushrut Trauma Centre,Ring Road, Behind I.P. College, Near Civil Lines Metro Station, Metcalf Road,Civil Lines,New Delhi,Delhi 110054,India: OK
    Geocoded: Deen Dayal Upadhyay Hospital,Clock Tower Chowk, Hari Enclave,Hari Nagar,New Delhi,Delhi 110064,India: OK
    Geocoded: Safdarjung Hospital,Safdarjung Campus,Ansari Nagar West,New Delhi,Delhi 110029,India: OK
    Geocoded: Lal Bahadur Shastri Hospital,Near Kalyanvas Colony, Mayur Vihar, Phase -II,Khichripur,New Delhi,Delhi 110091,India: OK
    Geocoded: GTB Hospital,GTB Enclave,Shahdara,New Delhi,Delhi 110095,India: OK



```python
df_delhi_hospitals = df_delhi_hospitals.iloc[[results.index(entry) for entry in results if entry['status'] == 'OK']]
```


```python
df_delhi_hospitals['lat'] = [entry['latitude'] for entry in results if entry['status'] == 'OK']
df_delhi_hospitals['lng'] = [entry['longitude'] for entry in results if entry['status'] == 'OK']
```


```python
df_delhi_hospitals['hosp_id'] = range(1, df_delhi_hospitals.shape[0] + 1)
```


```python
df_delhi_hospitals.to_csv(os.path.join(DATA_FOLDER, 'delhi_hospitals_final_geocoded.csv'), index=False)
```


```python
df_delhi_hospitals
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>city</th>
      <th>country</th>
      <th>name</th>
      <th>neighbourhood</th>
      <th>pin</th>
      <th>state</th>
      <th>street</th>
      <th>lat</th>
      <th>lng</th>
      <th>hosp_id</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>New Delhi</td>
      <td>India</td>
      <td>Ram Manohar Lohia Hospital</td>
      <td>Connaught Place</td>
      <td>110001</td>
      <td>Delhi</td>
      <td>Baba Kharak Singh Marg</td>
      <td>28.627123</td>
      <td>77.207337</td>
      <td>1</td>
    </tr>
    <tr>
      <th>1</th>
      <td>New Delhi</td>
      <td>India</td>
      <td>North DMC Medical College &amp; Hindu Rao Hospital</td>
      <td>Malka Ganj</td>
      <td>110007</td>
      <td>Delhi</td>
      <td>DR. J.S. Kkaranwal Memorial Road</td>
      <td>28.650374</td>
      <td>77.182668</td>
      <td>2</td>
    </tr>
    <tr>
      <th>2</th>
      <td>New Delhi</td>
      <td>India</td>
      <td>AIIMS</td>
      <td>Haus Khas</td>
      <td>110029</td>
      <td>Delhi</td>
      <td>Safdarjung Enclave, Aurobindo Marg, Ansari Nagar</td>
      <td>28.566827</td>
      <td>77.208120</td>
      <td>3</td>
    </tr>
    <tr>
      <th>3</th>
      <td>New Delhi</td>
      <td>India</td>
      <td>Sushrut Trauma Centre</td>
      <td>Civil Lines</td>
      <td>110054</td>
      <td>Delhi</td>
      <td>Ring Road, Behind I.P. College, Near Civil Lin...</td>
      <td>28.679779</td>
      <td>77.228379</td>
      <td>4</td>
    </tr>
    <tr>
      <th>4</th>
      <td>New Delhi</td>
      <td>India</td>
      <td>Deen Dayal Upadhyay Hospital</td>
      <td>Hari Nagar</td>
      <td>110064</td>
      <td>Delhi</td>
      <td>Clock Tower Chowk, Hari Enclave</td>
      <td>28.628012</td>
      <td>77.112397</td>
      <td>5</td>
    </tr>
    <tr>
      <th>5</th>
      <td>New Delhi</td>
      <td>India</td>
      <td>Safdarjung Hospital</td>
      <td>Ansari Nagar West</td>
      <td>110029</td>
      <td>Delhi</td>
      <td>Safdarjung Campus</td>
      <td>28.567839</td>
      <td>77.205795</td>
      <td>6</td>
    </tr>
    <tr>
      <th>6</th>
      <td>New Delhi</td>
      <td>India</td>
      <td>Lal Bahadur Shastri Hospital</td>
      <td>Khichripur</td>
      <td>110091</td>
      <td>Delhi</td>
      <td>Near Kalyanvas Colony, Mayur Vihar, Phase -II</td>
      <td>28.617721</td>
      <td>77.311242</td>
      <td>7</td>
    </tr>
    <tr>
      <th>7</th>
      <td>New Delhi</td>
      <td>India</td>
      <td>GTB Hospital</td>
      <td>Shahdara</td>
      <td>110095</td>
      <td>Delhi</td>
      <td>GTB Enclave</td>
      <td>28.683812</td>
      <td>77.311004</td>
      <td>8</td>
    </tr>
  </tbody>
</table>
</div>



# Crash & taxi Locations

## Real data availability assumptions

* This study's geographical area of interest spans the entire National Capital Region, which is comprised of a number of districts in Haryana, Rajasthan and Uttar Pradesh along with the National Capital Territory of Delhi. 
* Taxi location data is supposed to include log information of Delhi taxis reported at a frequency of 1 minute, spanning the entire duration of the study, which is [(12 am, 1st January 2016), (12 am, 1st January 2017))
* Crash information is sourced from fatal road accidents data in NCR for the year 2017. This dataset is supposed to have about 2000 records. 

## Simulated Data

* To avoid prohibitively long computation times, the simulated crash dataset will have only 100 records, with timepoints anywhere in [(12 am, 1st January 2016), (12 am, 1st January 2017))
* Simulated data for taxi locations will have 1 log for 10000 taxis for [(12 am, 30th December 2015), (12 am, 1st January 2016)).
* All events in this dataset will be from within the National Capital Territory of Delhi and not the whole NCR.

Using 'as the crow flies' distance to get the closest taxes from crash location will result in data that is not representative of actual travel distances. We need information on the road network. This study uses Open Street Map (OSM)'s road network information. This is accomplished with [osmnx](https://github.com/gboeing/osmnx), a Python package that does API calls to OSM API. We do not directly use Google Directions API, which has the capability to give estimated travel times based on time of the day, as other factors such as API call latency for each query and daily quota mean that we must restrict the number of these queries to as small as possible. An advantage of osmnx is that there will be only one OSM API call, when the entire road network dataset will be downloaded, and all shortest distance computations will be local.


```python
# dctlst_places = [ {'state':'National Capital Territory', 'country':'India'},
#                 {'district': 'Meerut', 'state':'Uttar Pradesh', 'country':'India'},
#                 {'district': 'Muzzafarnagar', 'state':'Uttar Pradesh', 'country':'India'},
#                 {'district': 'Ghaziabad', 'state':'Uttar Pradesh', 'country':'India'},
#                 {'district': 'Gautam Budh Nagar', 'state':'Uttar Pradesh', 'country':'India'},
#                 {'district': 'Bulandshahr', 'state':'Uttar Pradesh', 'country':'India'},
#                 {'district': 'Baghpat', 'state':'Uttar Pradesh', 'country':'India'},
#                 {'district': 'Hapur', 'state':'Uttar Pradesh', 'country':'India'},
#                 {'district': 'Shamli', 'state':'Uttar Pradesh', 'country':'India'},
#                 {'district': 'Faridabad', 'state': 'Haryana', 'country': 'India'},
#                 {'district': 'Gurugram', 'state': 'Haryana', 'country': 'India'},
#                 {'district': 'Mahendragarh', 'state': 'Haryana', 'country': 'India'},
#                 {'district': 'Bhiwani', 'state': 'Haryana', 'country': 'India'},
#                 {'district': 'Charkhi Dhadri', 'state': 'Haryana', 'country': 'India'},
#                 {'district': 'Nuh', 'state': 'Haryana', 'country': 'India'},
#                 {'district': 'Rohtak', 'state': 'Haryana', 'country': 'India'},
#                 {'district': 'Sonipat', 'state': 'Haryana', 'country': 'India'},
#                 {'district': 'Rewari', 'state': 'Haryana', 'country': 'India'},
#                 {'district': 'Jhajjar', 'state': 'Haryana', 'country': 'India'},
#                 {'district': 'Panipat', 'state': 'Haryana', 'country': 'India'},
#                 {'district': 'Palwal', 'state': 'Haryana', 'country': 'India'},
#                 {'district': 'Jind', 'state': 'Haryana', 'country': 'India'},
#                 {'district': 'Karnal', 'state': 'Haryana', 'country': 'India'},
#                 {'district': 'Alwar', 'state': 'Rajasthan', 'country': 'India'},
#                 {'district': 'Bharatpur', 'state': 'Rajasthan', 'country': 'India'}
#                 ]

dctlst_places = [ {'state':'National Capital Territory', 'country':'India'}]

ncr_roads = ox.graph_from_place(place_names, network_type='drive_service')
```

We then proceed with the download of NCR road network. We can save this as shape files. This will create two folders, Nodes and Edges. Edges have information on road segments and Nodes can be thought of as (lat, long) points that mark curves, intersections, etc., that is essential to reconstruct the roads. Since we will use these points as possible candidate locations for crashes and taxis, we also create a csv version of edges.


```python
ncr_network = gis_util.get_street_network(dctlst_places, DATA_FOLDER, 'ncr')
ncr_network = ox.load_graphml(filename='ncr.graphml', folder=os.path.join(DATA_FOLDER, 'ncr'))

gdf_ncr = gpd.read_file(os.path.join(DATA_FOLDER, 'ncr', 'nodes', 'nodes.shp'))

gdf_ncr.to_csv(os.path.join(DATA_FOLDER, 'ncr', 'ncr.csv'))
```


```python
df_delhi_points = pd.read_csv(os.path.join(DATA_FOLDER, 'ncr', 'ncr.csv'), index_col=0)
```


```python
df_delhi_points.columns
```




    Index(['highway', 'lat', 'lon', 'osmid', 'ref', 'geometry'], dtype='object')



Creating crash data with 2000 random points (with replacement) from the nodes csv and 2000 time points in 2016,


```python
crash_loc_indices = np.random.choice(df_delhi_points.index, size=2000, replace=True)
```


```python
time_points = pd.date_range(start='2016-01-01 00:00:00', end='2017-01-01 00:00:00', freq='15min', tz='Asia/Kolkata')
```


```python
df_crash_events = pd.DataFrame({'event_id': range(1, 2001),
                                'event_time': np.random.choice(time_points, size=2000, replace=True), 
                                'event_lat': df_delhi_points.iloc[crash_loc_indices]['lat'],
                                'event_lng': df_delhi_points.iloc[crash_loc_indices]['lon']
                              })
```

Localising the timepoints to IST,


```python
df_crash_events['event_time'] = pd.to_datetime(df_crash_events['event_time']).dt.tz_localize('Asia/Kolkata')
```


```python
df_crash_events.to_csv(os.path.join(DATA_FOLDER, 'crashes.csv'), index=False)
```

Creating 10000 taxis and creating one log entry for each them at a random time point (with replacement) in [(12 am, 30th December 2015), (12 am, 1st January 2016)), with 10000 random locations chosen from edges csv file with replacement,


```python
taxi_loc_indices = np.random.choice(df_delhi_points.index, size=10000, replace=True)
time_points = pd.date_range(start='2015-12-29 00:00:00', end='2015-12-31 23:59:00', freq='0.5min', tz='Asia/Kolkata')
df_taxi_log = pd.DataFrame({'log_id': range(1, 10001),
                            'taxi_id': range(1, 10001),
                            'log_time': np.random.choice(time_points, size=10000, replace=True), 
                            'log_lat': df_delhi_points.iloc[taxi_loc_indices]['lat'],
                            'log_lng': df_delhi_points.iloc[taxi_loc_indices]['lon']
                            })
df_taxi_log['log_time'] = pd.to_datetime(df_taxi_log['log_time']).dt.tz_localize('Asia/Kolkata')
df_taxi_log['log_on_call'] = True
```

Since each taxi is assumed to have numerous logs, and it makes sense to restrict it to taxis which are available for hire, we introduce the following fields:

* log_id - Uniquely identify each log. Helps keep track of multiple spatiotemporal logs for each taxi.
* log_on_call - To denote the taxi's availability


```python
df_taxi_log.to_csv(os.path.join(DATA_FOLDER, 'taxi_log.csv'), index=False)
```

# Getting closest hospital & taxi information

Loading all the files created in the steps above,


```python
df_delhi_hospitals = pd.read_csv(os.path.join(DATA_FOLDER, 'delhi_hospitals_final_geocoded.csv'))
df_crash_events = pd.read_csv(os.path.join(DATA_FOLDER, 'crashes.csv'))
df_taxi_log = pd.read_csv(os.path.join(DATA_FOLDER, 'taxi_log.csv'))


df_taxi_log['log_time'] = pd.to_datetime(df_taxi_log['log_time']).dt.tz_localize('UTC').dt.tz_convert(
    'Asia/Kolkata')
df_crash_events['event_time'] = pd.to_datetime(df_crash_events['event_time']).dt.tz_localize(
    'UTC').dt.tz_convert('Asia/Kolkata')

ncr_network = ox.load_graphml(filename='ncr.graphml', folder=os.path.join(DATA_FOLDER, 'ncr'))

gdf_ncr = gpd.read_file(os.path.join(DATA_FOLDER, 'ncr', 'nodes', 'nodes.shp'))
```

Our problem, finding closest taxis and hospitals to crash locations, is similar to the modified closest pair of points. We have two sets (Crashes, Hospitals) or (Crashes, Taxis), and our prerogative is to match each point in set A (Crashes) with the closest point in set B (Hospitals or Taxis). Naive approaches will mean there will be (nCrash X nHospital + nCrash X nTaxi) computations. 

In order to reduce the number of computations, we make use of [Pandana](https://github.com/UDST/pandana), which makes use of efficient closest pair calculation algorithms for netowork related queries like ours. A noteworthy feature of Pandana is its general network level aggregations. The impication is that there will only be a single query (1 each for taxi and hospital) for each crash, on the aggregated list of closest points of interest to each node. This aggregated list can be thought of a list of all nodes in the network, along with information on k closest points of interest to each of them. These points of interest can be hospitals or taxi locations.

Let us visualise information on roads provided by OSM.


```python
gdf_nodes = ox.graph_to_gdfs(ncr_network, edges=False)
gdf_edges = ox.graph_to_gdfs(ncr_network, nodes=False)
```


```python
gdf_edges.head(n=5)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>access</th>
      <th>area</th>
      <th>bridge</th>
      <th>geometry</th>
      <th>highway</th>
      <th>key</th>
      <th>landuse</th>
      <th>lanes</th>
      <th>length</th>
      <th>maxspeed</th>
      <th>name</th>
      <th>oneway</th>
      <th>osmid</th>
      <th>ref</th>
      <th>service</th>
      <th>tunnel</th>
      <th>u</th>
      <th>v</th>
      <th>width</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>LINESTRING (77.1680173 28.5426036, 77.16793149...</td>
      <td>residential</td>
      <td>0</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>62.355284</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>False</td>
      <td>7892104</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>58047704</td>
      <td>58047707</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>1</th>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>LINESTRING (77.16769119999999 28.5430787, 77.1...</td>
      <td>residential</td>
      <td>0</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>64.212083</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>False</td>
      <td>7892369</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>58047707</td>
      <td>58051020</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>2</th>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>LINESTRING (77.16769119999999 28.5430787, 77.1...</td>
      <td>residential</td>
      <td>0</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>62.355284</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>False</td>
      <td>7892104</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>58047707</td>
      <td>58047704</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>3</th>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>LINESTRING (77.16769119999999 28.5430787, 77.1...</td>
      <td>residential</td>
      <td>0</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>23.925042</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>False</td>
      <td>7892369</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>58047707</td>
      <td>2265700198</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>4</th>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>LINESTRING (77.16437809999999 28.5383963, 77.1...</td>
      <td>residential</td>
      <td>0</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>48.454379</td>
      <td>NaN</td>
      <td>JNU Ring Road</td>
      <td>False</td>
      <td>7892285</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>58049717</td>
      <td>4231408578</td>
      <td>NaN</td>
    </tr>
  </tbody>
</table>
</div>



Closest pair of points in graphs (network) use weights (impedences) on nodes as the flavour in which they are close; the flavour in our case is distance or time taken to travel. Distance alone cannot give a true estimate of required time, since a trip that makes use of too many short side lanes can be slower than a longer path on an expressway. Speed limit thus is valuable information. This however is not available for this road network. We still can mimic this by assigning limits based on the type of the segments. This can then be used to weight the edges. Checking the types of road segments present,


```python
gdf_edges['highway'] = list(map(str, gdf_edges['highway']))
```


```python
gdf_edges['highway'].unique()
```




    array(['residential', 'unclassified', 'secondary', 'primary',
           'primary_link', 'tertiary', 'tertiary_link', 'service',
           'trunk_link', 'trunk', 'secondary_link', 'living_street',
           "['service', 'secondary']", 'motorway', 'motorway_link',
           "['service', 'residential']", "['unclassified', 'residential']",
           "['tertiary', 'residential']", "['tertiary', 'secondary']",
           "['residential', 'living_street']", "['primary', 'secondary']",
           "['trunk', 'trunk_link']", "['trunk', 'secondary']",
           "['residential', 'service']", "['tertiary', 'trunk_link']",
           "['secondary_link', 'primary']", "['unclassified', 'secondary']",
           "['primary_link', 'secondary']", "['unclassified', 'service']",
           "['unclassified', 'tertiary']", "['secondary_link', 'secondary']",
           "['unclassified', 'living_street']",
           "['service', 'living_street']", "['tertiary', 'road']", 'road',
           "['tertiary', 'unclassified']"], dtype=object)




```python
gdf_edges['maxspeed'] = list(map(util.tidy_maxspeed_tuple_to_int, gdf_edges['maxspeed']))
```

Some road segments do have max speed information.


```python
gdf_edges.loc[gdf_edges['maxspeed'].notnull()].head(n=5)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>access</th>
      <th>area</th>
      <th>bridge</th>
      <th>geometry</th>
      <th>highway</th>
      <th>key</th>
      <th>landuse</th>
      <th>lanes</th>
      <th>length</th>
      <th>maxspeed</th>
      <th>name</th>
      <th>oneway</th>
      <th>osmid</th>
      <th>ref</th>
      <th>service</th>
      <th>tunnel</th>
      <th>u</th>
      <th>v</th>
      <th>width</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>241</th>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>LINESTRING (77.2388844 28.5780587, 77.2388935 ...</td>
      <td>residential</td>
      <td>0</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>302.863721</td>
      <td>20</td>
      <td>Prachin Shiv Mandir Road</td>
      <td>False</td>
      <td>38116089</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>250100205</td>
      <td>250100312</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>266</th>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>LINESTRING (77.24312279999999 28.57865, 77.241...</td>
      <td>residential</td>
      <td>0</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>168.725184</td>
      <td>20</td>
      <td>Prachin Shiv Mandir Road</td>
      <td>False</td>
      <td>38116089</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>250100307</td>
      <td>250100312</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>268</th>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>LINESTRING (77.24312279999999 28.57865, 77.245...</td>
      <td>residential</td>
      <td>0</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>682.872896</td>
      <td>20</td>
      <td>Prachin Shiv Mandir Road</td>
      <td>False</td>
      <td>38116089</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>250100307</td>
      <td>448444374</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>270</th>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>LINESTRING (77.2413954 28.5786133, 77.24312279...</td>
      <td>residential</td>
      <td>0</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>168.725184</td>
      <td>20</td>
      <td>Prachin Shiv Mandir Road</td>
      <td>False</td>
      <td>38116089</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>250100312</td>
      <td>250100307</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>272</th>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>LINESTRING (77.2413954 28.5786133, 77.2413382 ...</td>
      <td>residential</td>
      <td>0</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>302.863721</td>
      <td>20</td>
      <td>Prachin Shiv Mandir Road</td>
      <td>False</td>
      <td>38116089</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>250100312</td>
      <td>250100205</td>
      <td>NaN</td>
    </tr>
  </tbody>
</table>
</div>



Getting the minimum maxspeed for each category of highways,


```python
dct_min_speed_by_category = dict(gdf_edges.loc[gdf_edges['maxspeed'].notnull()].groupby('highway')['maxspeed'].min())
dct_min_speed_by_category
```




    {"['residential', 'service']": 30.0,
     "['secondary', 'primary']": 40.0,
     "['trunk', 'secondary']": 40.0,
     "['trunk', 'trunk_link']": 40.0,
     "['unclassified', 'tertiary']": 20.0,
     'living_street': 10.0,
     'motorway': 40.0,
     'motorway_link': 40.0,
     'primary': 40.0,
     'primary_link': 40.0,
     'residential': 10.0,
     'secondary': 20.0,
     'service': 20.0,
     'tertiary': 20.0,
     'tertiary_link': 30.0,
     'trunk': 40.0,
     'trunk_link': 60.0,
     'unclassified': 20.0}



Since we already have representative lowerbound speed limits for each of the categories, we can use this to impute the missing speed limits. Assuming the worst case, I proceed to replace missing maxspeed values with the least maxspeed value seen for that particular category.


```python
for idx, row in gdf_edges.loc[gdf_edges['maxspeed'].isnull()].iterrows():
    maxspeed = dct_min_speed_by_category.get(row['highway'], np.nan)
    if math.isnan(maxspeed):
        try:
            combo_types = ast.literal_eval(row['highway'])
            maxspeed = min([speed for speed in [dct_min_speed_by_category.get(highwaytype, np.nan) for highwaytype in combo_types] if not math.isnan(speed)])
            
        except:
            pass
            
    gdf_edges.loc[idx, 'maxspeed'] = maxspeed
```


```python
gdf_edges.loc[gdf_edges['maxspeed'].isnull()]['highway'].unique()
```




    array(['secondary_link', 'road'], dtype=object)



Since primary_link has a speed limit of 40 and tertiary_link 30, we can fix secondary_link to a limit of 35.


```python
dct_min_speed_by_category['secondary_link'] = 35
gdf_edges.loc[gdf_edges['highway'] == 'secondary_link', 'maxspeed'] = 35
```

Since the rest of the segments are types, let us assign the lowest limit possible based on the types in the combo. For example, for an [A, B], let us assign min(min(A), min(B))

Road is the only type that has no maxspeed. Let us investigate what these segments usually mean.


```python
gdf_edges.loc[gdf_edges['highway'] == 'road', 'name'].unique()
```




    array([nan, 'Upper gali'], dtype=object)



'Gali' is Hindi equivalent of street, but not as broad as the Hindi designation for broad streets, पथ (transliterated as 'path'). Also, OSM uses 'road' for a varied classification of path segments. So this is possibly a non homogeneous set. For this reason, I am assigning it a level higher than the lowest designation, residential, assigning it a maxspeed of 15. I do not assign it the lowermost designation, as that would mean that residential segments would then have the same preference as roads.


```python
dct_min_speed_by_category['road'] = 15
gdf_edges.loc[gdf_edges['highway'] == 'road', 'maxspeed'] = 15
```

Let us construct the time taken to traverse weights,


```python
gdf_edges['time_to_traverse'] = gdf_edges['length']/ gdf_edges['maxspeed']
```


```python
ox.save_load.save_gdf_shapefile(gdf_edges, filename="edges_edited.shp", folder=os.path.join(DATA_FOLDER, 'ncr'))
```

We are now all set to proceed with setting up a Pandana network. We use this network for network related queries such as closest hospital and taxis from crash nodes.


```python
twoway = list(~gdf_edges["oneway"].values)
pdnet_ncr=pdna.Network(gdf_nodes["x"], gdf_nodes["y"], gdf_edges["u"], gdf_edges["v"],
                 gdf_edges[["time_to_traverse"]], twoway=twoway)
```

## Closest Pair matching computation

Pandanas allows aggregated network information related queries on road networks with custom points by allowing new 'points of interest' overlayed on the base network. Closest node without any impedence to each of these 'points of interests' are determined, and these base nodes will then be used to compute distances from other nodes with impedence. For example, say we have a POI 'XYZ Hospital' to be added to Delhi network. Pandanas would choose the closest 'as the crow flies' node to this POI and assign this node as this POI's home node. Then, each of the other nodes' distance from it will be calculated from this node, taking account of usual impedences, such as length or maxspeed of edges connecting them. Expanding this to a list of POIs, pandana's one feature is to compute an aggregated list table for all nodes with k closest POIs to them. This table can be used to get closest POIs (say hospitals) from our nodes of interest, (say crash locations)

Converting the street network to a pandana network object and adding hospitals as 'POI' to them,


```python
pdnet_ncr.set_pois("hospitals", 1000000, 3, df_delhi_hospitals['lng'], df_delhi_hospitals['lat'])
```

* To accelerate this step, and since this has a great potential to be done in parallel, we use multiprocessing.
* A situation to take note of, when allotting taxis to crashes, is that there can be a timepoint when multiple crashes occur all at once. With non-availability of taxis, the response time should be high. 
* In order to account for the situation above, we can divide the crashes into multiple chunks for parallel processing in such a way that the crashes between chunks are separated by at least 6 hours.
* The present version of code doesn't account for the situation described above.

# Computation system modules:

## Google keys repository management:

1. A dictionary object is used to keep track of the last time a key was used and number of times it was accessed in the day it was used for the last time.
2. Given a list of keys, this module will return a key that either hasn't been used in the past 24 hours, or a key which hasn't been used more than the API limit.
3. If no such key is present, this module will make the entire execution process wait till the earliest last accessed key's quota is renewed. This key will be returned. For example, say we have keys (1, 2, 3) whose quota have all been exhausted. Lets say key 2 was the earliest last call time. So the program will wait till 24 hours after this last call time and return key 2.


## Get the next weekday: 

An underlying assumption is that trip durations have weekly and hourly patterns. Since trip duration is fetched from Google Directions API, which does not allow historic travel time requests, and since crash events are all in the past, trip duration calculator module will send requests for days in the future which have the same weekday and time of the day as the crash event. The current logic will post date it to a suitable timepoint within a week from the time the API call is made. For example, if crash happened on Dec 1, 2016, 9 AM IST, which falls on a Thursday, API call request will have the date set to Mar 1, 2018, 9 AM IST. 

## Get nearest POIs:

**Arguments:** crash list,<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Streetnetwork graph,<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Poi name (can be hospitals or taxis,<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;impedence - measure that determines distance between points,<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;npoi - number of closest pois to be returned for further analysis with google API,<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;poi location logs - Taxi logs,<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;taxi ids - list of taxi ids part of the program<br/>
                      

**Returns:** npoi pois for each crash<br/>

This submodule matches each crash event with npoi pois which are closest to the crash location. Time taken to travel is the metric used in this calculation. Pandana's spatial aggregation feature that makes use of closest pair of nodes in a graph calculator algorithms make it easier to arrive at this without resorting to ncrash $*$ ntaxi or  ncrash $*$ nhospital graph distance computations. While closest hospital calculation is straightforward, for taxis, the query set must be restricted to taxis that are available as per the logs prior to the crash event. An important assumption is that the taxi logs are assumed to be at 1 minute frequency, and all taxis report their availability every 1, despite their being on or off service. 

## Get shortest travel times:

**Arguments:** crash & closest poi matches - returned by nearest POI calculator, <br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;dct_key_stats - google API key usage information register,<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;crash list,<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;poi logs,<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;api keys,<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;api_name - Google Directions,<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;api_limit - 2500 for Google Directions every 24 hrs,<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;poi_lat_col - column name in poi log table which has latitude info,<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;poi_lng_col - column name in poi log table which has longitude info,<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;poi_id_col - column name in poi log table which has log id<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;crash list,<br/>
                      

**Returns** : minimum time required to get to the closest POI from each crach spot. This comprises of 3 possible values, corresponding to 'best_guess', 'optimistic' and 'pessimistic' scenarios.

Three API calls are made for each crash - POI pair, to fetch 'best_guess', 'optimistic' and 'pessimistic' times. The shortest times in each scenario, among all the POIs matched to a crash, are returned from this block.


Putting all the above modules together,

![title](modules.png)
