from google.cloud import bigquery, firestore
import json
import pandas as pd
import time
import requests
import geojson
import numpy as np 
from matplotlib.path import Path
from time import sleep


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


def bbox_to_vertices(bbox):
    if bbox is None:
        return None

    vertices = [
        (bbox['north'], bbox['west']),
        (bbox['north'], bbox['east']),
        (bbox['south'], bbox['east']),
        (bbox['south'], bbox['west'])
    ]

    return vertices


def get_region_bbox(region):
    return get_all_region_info()[region]['bbox']


def get_region_vertices(region):
    return bbox_to_vertices(get_region_bbox(region))


def isQueryInBoundingBox(bounding_box_vertices, query_lat, query_lon):
    verts = [(0, 0)] * len(bounding_box_vertices)
    for elem in bounding_box_vertices:
        verts[elem[0]] = (elem[2], elem[1])
    # Add first vertex to end of verts so that the path closes properly
    verts.append(verts[0])
    codes = [Path.MOVETO]
    codes += [Path.LINETO] * (len(verts) - 2)
    codes += [Path.CLOSEPOLY]
    boundingBox = Path(verts, codes)
    return boundingBox.contains_point((query_lon, query_lat))


def getAreaModelByLocation(lat, lon, string=None):
    area_models = get_all_region_info()
    if string is None:
        for key in area_models:
            if isQueryInBoundingBox(area_models[key]['bbox'], lat, lon):
                return area_models[key]
    else:
        try:
            return area_models[string]
        except:
            return None


def applyRegionalLabelsToDataFrame(df, null_value=np.nan, trim=False):
    df['Label'] = null_value

    for region_name, region_info in get_all_region_info().items():
        
        bbox = get_region_bbox(region_name)
        
        if bbox is None:
            continue 

        df.loc[
            (df['Lat'] >= bbox['south']) &
            (df['Lat'] <= bbox['north']) &
            (df['Lon'] >= bbox['west']) &
            (df['Lon'] <= bbox['east']),
            'Label'
        ] = region_info['name']

    if trim:
        x = len(df)
        df = df.dropna(subset=['Label'])
        print(f"Dropped {x - len(df)} unlabeled rows.")
    return df


def chunk_list(ls, chunk_size=10000):
    '''
    BigQuery only allows inserts <=10,000 rows
    '''
    for i in range(0, len(ls), chunk_size):
        yield ls[i: i + chunk_size]


def setPMSModels(df, col_name):
    pms_models = ['PMS1003', 'PMS3003', 'PMS5003', 'PMS7003']
    for model in pms_models:
        df.loc[df['Type'].str.contains(model), col_name] = model
    return df


def setChildFromParent(df, pairings, col_name):
    df.loc[pairings.index, col_name] = df.loc[pairings, col_name].values
    return df


def getParentChildPairing(df):
    '''
    Purple Air devices have two PM sensors inside. Data is reported for both sensors seperately,
    but one sensor is considered the "parent" and one is the "child". The child has
    lots of missing information, like DEVICE_LOCATIONTYPE, Flag, Type. So we create
    this Series to link parents and children, then later use this Series to fill in
    missing data for the children with data from their parents. 
    
    Beware: sometimes we find orphans - rows with a non-null ParentID, but no corresponding 
    row with an ID equal to the value of that ParentID. 
    '''

    # Get the rows where ParentID is not Null (ParentID values are the IDs of the parent sensors)
    pairings = df['ParentID'].loc[~df['ParentID'].isnull()].astype(int)

    # Eliminate orphans (sorry orphans)
    pairings = pairings[pairings.isin(df.index)]

    return pairings


def main(data, context):

    response = None
    try:
        response = json.loads(requests.get('https://www.purpleair.com/json?a').text)
        results = response['results']
    except Exception as e:
        print('Could not download data. Exception: ', str(e), response)
        try:
            print('trying again after 15 seconds')
            sleep(20)
            response = json.loads(requests.get('https://www.purpleair.com/json?a').text)
            results = response['results']
        except:
            print('Could not download data (take 2). Exception: ', str(e), response)
            return
            

    # Convert JSON response to a Pandas DataFrame
    df = pd.DataFrame(results)
    print(f'rows: {len(df)}')

    if df.empty:
        return

    # Trim off old data
    df['LastSeen'] = pd.to_datetime(df['LastSeen'], unit='s', utc=True)

    # Remove all datapoints older than 6 minutes. Run every 5 minutes, so give 1 minute overlap
    df = df[df['LastSeen'] > (pd.Timestamp.utcnow() - pd.Timedelta(6, unit='minutes'))]

    # Following Series operations depend on having an index
    df = df.set_index('ID')

    # Get Series with index = child ID and 'ParentID' = Parent ID
    pairing = getParentChildPairing(df)
    
    # Set DEVICE_LOCATIONTYPE child to DEVICE_LOCATIONTYPE parent
    df = setChildFromParent(df, pairing, 'DEVICE_LOCATIONTYPE')

    # Set flag of child sensor to flag of parent sensor
    df = setChildFromParent(df, pairing, 'Flag')

    # Set the 'Type' string of child to that of parent
    df = setChildFromParent(df, pairing, 'Type')

    # Use 'Flag', 'A_H', 'Hidden' to filter out bad data
    #   'Flag': Data flagged for unusually high readings
    #   'A_H': true if the sensor output has been downgraded or marked for attention due to suspected hardware issues
    #   'Hidden': Hide from public view on map: true/false
    df = df.fillna({
        'A_H': False, 
        'Flag': False, 
        'Hidden': False
        })
    
    # Convert JSON 'true'/'false' strings into bools
    df = df.replace({'true': True, 'false': False})

    # Change types
    df = df.astype({
        'A_H': bool, 
        'Flag': bool, 
        'Hidden': bool,
        'PM2_5Value': float,
        'temp_f': float,
        'LastSeen': str,
        })

    # If any of these are true, remove the row
    df['Flag'] = df['Flag'] | df['A_H'] | df['Hidden']

    # Remove rows
    df = df[df['DEVICE_LOCATIONTYPE'] == 'outside']     # Remove sensors not outside
    df = df[df['Flag'] != 1]                            # Remove sensors with Flag, A_H, or Hidden flags
    df = df.dropna(subset=['Lat', 'Lon'])               # Remove sensors with no lat/lon info

    # Apply regional labels ('slc_ut', 'chatt_tn', etc.)
    df = applyRegionalLabelsToDataFrame(df, trim=True)

    # Create the GPS column
    df['GPS'] = df.apply(lambda x: geojson.dumps(geojson.Point((x['Lon'], x['Lat']))), axis=1)

    # Move bad PM data out of cleaned column
    # df['PM2_5_Raw'] = df.loc[df['PM2_5Value'] >= float(env['pm_threshold']), 'PM2_5Value']
    # df['Flags'] = 0
    # df.loc[df['PM2_5Value'] >= float(env['pm_threshold']), 'Flags'] |= 2
    # df.loc[df['PM2_5Value'] >= float(env['pm_threshold']), 'PM2_5Value'] = np.nan

    # Convert temperature F to C
    df['temp_f'] = (df['temp_f'] - 32) * (5. / 9)

    # clean up PMS 'Type' names
    df = setPMSModels(df, col_name='PMSModel')

    # Reduce DataFrame to desired columns
    df = df.reset_index()
    cols_to_keep = ['LastSeen', 'ID', 'GPS', 'PM2_5Value', 'PM2_5_Raw', 'Flags', 'PMSModel', 'humidity', 'temp_f', 'pressure', 'Label']
    df = df.loc[:, df.columns.isin(cols_to_keep)]

    # Append 'PP' to device id's
    df['ID'] = 'PP' + df['ID'].astype(str)

    # Add 'Source' = 'PurpleAir'
    df['Source'] = 'PurpleAir'

    # Finally, convert NaN to None
    df = df.replace({np.nan: None})

    # Rename columns
    df = df.rename({
        'LastSeen':     'Timestamp',
        'ID':           'DeviceID', 
        'PM2_5Value':   'PM2_5',
        'humidity':     'Humidity', 
        'temp_f':       'Temperature',
        'pressure':     'Pressure'
    }, axis='columns')


    # Convert dataframe to list of dicts
    data = df.to_dict('records')

    # Create unique row_ids to avoid duplicates when inserting overlapping data
    row_ids = pd.util.hash_pandas_object(df[['Timestamp', 'DeviceID']]).values.astype(str)

    bq_client = bigquery.Client()
    target_table = bq_client.dataset('telemetry').table('telemetry')
    
    # Maximum upload size for BigQuery API is 10,000 rows, so we have to upload the data in chunks
    for i, (data_chunk, rows_chunk) in enumerate(zip(chunk_list(data, chunk_size=10000), chunk_list(row_ids, chunk_size=10000))):
        errors = bq_client.insert_rows_json(
            table=target_table,
            json_rows=data_chunk,
            row_ids=rows_chunk
        )
        if errors:
            print(errors)
        else:
            print(f"Inserted {len(rows_chunk)} rows")


if __name__ == '__main__':
    main('data', 'context')