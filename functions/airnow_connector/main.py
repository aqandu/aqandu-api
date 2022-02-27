"""
We use airnow.gov API
"""


from google.cloud import bigquery, firestore
import time
import json
import pandas as pd
import requests
import os
import datetime
import geojson
import numpy as np 
import yaml


# Globals
bq_client = bigquery.Client()
# gs_client = storage.Client()
env = None 
_area_params = None

# api_key = os.environ['api_key']
UPDATE_MINS = 60


# def getProjectConfigFile():
#     if not hasattr(getProjectConfigFile, "updateTime"):
#         getProjectConfigFile.updateTime = 0
#     if time.time() - getProjectConfigFile.updateTime > (UPDATE_MINS*60):
#         getProjectConfigFile.env = yaml.load(gs_client.get_bucket(os.environ['gs_bucket']).get_blob(os.environ['config']).download_as_string(), Loader=yaml.SafeLoader)
#         getProjectConfigFile.updateTime = time.time()
#     return getProjectConfigFile.env


# def getAreaParams():
#     if not hasattr(getAreaParams, "updateTime"):
#         getAreaParams.updateTime = 0
#     if time.time() - getAreaParams.updateTime > (UPDATE_MINS*60):
#         bucket = env['storage']['server_bucket']['name']
#         area_params_json = env['storage']['server_bucket']['files']['area_params'] 
#         area_params = json.loads( 
#             gs_client.get_bucket(bucket).get_blob(area_params_json).download_as_string())
#         getAreaParams.params = buildAreaModelsFromJson(area_params)
#         getAreaParams.updateTime = time.time()
#     return getAreaParams.params

def get_all_region_info():
    if not hasattr(get_all_region_info, "updateTime"):
        get_all_region_info.updateTime = 0
        get_all_region_info.collection = firestore.Client().collection('region_info')
        get_all_region_info.params = None

    if time.time() - get_all_region_info.updateTime > (3600):
        
        params = {doc_ref.id: doc_ref.to_dict() for doc_ref in get_all_region_info.collection.stream()}
        
        get_all_region_info.updateTime = time.time()
        get_all_region_info.params = params

    return get_all_region_info.params


# def loadBoundingBox(bbox_info):
#     if bbox_info is None:
#         return None
#     rows = [row for row in bbox_info]
#     bounding_box_vertices = [(index, float(row['Latitude']), float(row['Longitude'])) for row, index in zip(rows, range(len(rows)))]
#     return bounding_box_vertices

def bbox_to_vertices(bbox):
    if bbox is None:
        return None

    vertices = [
        (0, bbox['north'], bbox['west']),
        (1, bbox['north'], bbox['east']),
        (2, bbox['south'], bbox['east']),
        (3, bbox['south'], bbox['west'])
    ]

    return vertices


def buildAreaModelsFromJson(json_data):
    area_models = {}
    for key in json_data:
        this_model = {}
        this_model['name'] = json_data[key]['name']
        this_model['timezone'] = json_data[key]['timezone']
        this_model['note'] = json_data[key]['note']
        this_model['elevationinterpolator'] = None
        this_model['bbox'] = bbox_to_vertices(json_data[key]['bbox'])
        if 'Source table map' in json_data[key]:
            this_model['sourcetablemap'] = json_data[key]['Source table map']
        area_models[key] = this_model
    return area_models


# def getAreaModelBounds(area_model):
#     area_bounds = area_model['bbox']
#     if area_bounds is None:
#         return None
#     # bounds = {
#     #     'lat_hi': area_bounds[0][1],
#     #     'lon_hi': area_bounds[1][2],
#     #     'lat_lo': area_bounds[2][1],
#     #     'lon_lo': area_bounds[3][2]
#     # }
    
#     # if bounds['lat_hi'] <= bounds['lat_lo'] or bounds['lon_hi'] < bounds['lon_lo']:
#     #     return None
#     else:
#         return area_bounds


def applyRegionalLabelsToDataFrame(df, region):
    df['Label'] = region['name']
    return df


def chunk_list(ls, chunk_size=10000):
    '''
    BigQuery only allows inserts <=10,000 rows
    '''
    for i in range(0, len(ls), chunk_size):
        yield ls[i: i + chunk_size]


def setPMSModels(df, col_name):
    df[col_name] = "FEM"
    return df


def getRowIDs(df):
    return  pd.util.hash_pandas_object(df[['Timestamp', 'DeviceID']]).values.astype(str)


def renameColumns(df):
    cols = {
        'UTC': 'Timestamp',
        'AgencyName': 'Source',
        'CO': 'CarbonMonoxide',
        'NO2': 'NitrogenDioxide',
        'OZONE': 'Ozone',
        'SO2': 'SulfurDioxide',
        'PM2.5': 'PM2_5',
        'SiteName': 'DeviceID'
    }
    return df.rename(columns=cols)


def queryRegion(region):

    if region['bbox'] is None:
        return

    base = "https://www.airnowapi.org/aq/data/?"
    params = "OZONE,PM25,PM10,CO,NO2,SO2"
    extras = "dataType=C&format=application/json&verbose=1&nowcastonly=0&includerawconcentrations=0"

    end = datetime.datetime.utcnow() - datetime.timedelta(hours=1)
    start = end - datetime.timedelta(hours=1)
    start_s = start.strftime("%Y-%m-%dT%H")
    end_s = end.strftime("%Y-%m-%dT%H")

    N = region['bbox']['north']
    S = region['bbox']['south']
    E = region['bbox']['east']
    W = region['bbox']['west']

    query = f"""{base}startDate={start_s}&endDate={end_s}&parameters={params}&BBOX={W},{S},{E},{N}&{extras}&API_KEY={os.environ['api_key']}"""

    response = requests.get(query)
    df = pd.read_json(response.text)

    if len(df) == 0:
        return None
    
    df = pd.pivot_table(
        df, 
        values='Value', 
        index=['UTC', 
        'SiteName', 'Latitude', 'Longitude', 'AgencyName'], 
        columns=['Parameter'], 
        aggfunc=np.mean,
        fill_value=np.nan
    )

    df = df.reset_index()

    df = setPMSModels(df, 'PMSModel')
    
    df = applyRegionalLabelsToDataFrame(df, region)

    df = renameColumns(df)

    df['GPS'] = df.apply(lambda x: geojson.dumps(geojson.Point((x['Longitude'], x['Latitude']))), axis=1)

    df = df.replace({np.nan: None})

    df = df.drop(columns=['Latitude', 'Longitude'])

    data = df.to_dict('records')

    row_ids = getRowIDs(df)

    target_table = bq_client.dataset('telemetry').table('telemetry')
    
    errors = bq_client.insert_rows_json(
        table=target_table,
        json_rows=data,
        row_ids=row_ids
    )
    if errors:
        print(errors)
    else:
        print(f"Inserted {len(data)} rows")


def main(data, context):

    global env, _area_params
    # env = getProjectConfigFile()
    _area_params = get_all_region_info()
    
    for k, v in _area_params.items():
        queryRegion(v)

if __name__ == '__main__':
    import os
    os.environ['api_key'] = "9BBAAFD4-B38F-468F-8AA4-DB2DBB36658B"
    main('dat', 'context')
