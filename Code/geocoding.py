import pandas as pd
import numpy as np
import requests
import json


def geocode(address_or_zipcode):
    global i
    i=i+1
    api_key = 'AIzaSyDBf6w3sNXqF9eLWhqqvaC29DJnurvrhhI'
    results,lat, lon = None, None,None
    base_url = "https://maps.googleapis.com/maps/api/geocode/json"
    endpoint = f"{base_url}?address={address_or_zipcode}&key={api_key}"

    print(endpoint)
    quit()

    # see how our endpoint includes our API key? Yes this is yet another reason to restrict the key
    r = requests.get(endpoint)
    if r.status_code not in range(200, 299):
        return None, None
    try:
        '''
        This try block incase any of our inputs are invalid. This is done instead
        of actually writing out handlers for all kinds of responses.
        '''
        results = r.json()['results'][0]
        lat = results['geometry']['location']['lat']
        lon = results['geometry']['location']['lng']
        print(i, lat, lon)
    except:
        pass
        print(i, 'failed: ',endpoint)
    return [results, lat, lon]


Neave_Key1='AIzaSyCvoebhRCT92rRG3HH7ITw4MmI6fiFBXOs'
Neave_Key2='AIzaSyAA0xTxqlSkDx3bwP96-SIlKaGe38xl4LA'
Ollie_key = 'AIzaSyDBf6w3sNXqF9eLWhqqvaC29DJnurvrhhI'

def execute_chunk(api_key,chunk_size):

    df=pd.read_csv('Data/Clean/CoC.csv')[['index','street','neighbourhood']]
    done=pd.read_csv('Data/Clean/CoC_geocoded.csv')
    chunk=df.drop(done['index']).sort_values(by=['neighbourhood'])[:chunk_size]

    chunk['address']=chunk['street']+', '+chunk['neighbourhood']+', BOGOTÁ D.C.'#+", Colombia"
    chunk['address']=chunk['address'].str.lstrip().str.replace('#',' ')

    chunk['geocoding_results'], chunk['lat'], chunk['lon']=zip(*chunk['address'].map(geocode))
    done=done.append(chunk)
    done.to_csv('Data/Clean/CoC_geocoded.csv', index=None)

i=0

for r in range(0,80):
    if r<40:
        execute_chunk(Neave_Key1,1000)
    if r>=40:
        execute_chunk(Neave_Key2,1000)
quit()
import ee
import geemap
import geopandas as gpd

ee.Initialize()
CoC=pd.read_csv('Data/Clean/CoC_geocoded.csv')
CoC=CoC.drop(columns={'geocoding_results'})

CoC=CoC.dropna(subset={'lat','lon'})

gdf = gpd.GeoDataFrame(
    CoC, 
    crs='EPSG:4326', 
    geometry = gpd.points_from_xy(
        CoC['lon'], 
        CoC['lat']
    )
)

# convert it into geo-json 
json_df = json.loads(gdf.to_json())

# create a gee object with geemap
ee_object = geemap.geojson_to_ee(json_df)

task_config = {
    'collection': ee_object, 
    'description':'Bogota_CoC_data',
    'assetId': 'users/ollielballinger/Colombia/Bogota_CoC_data'
}
task = ee.batch.Export.table.toAsset(**task_config)
#ee.data.deleteAsset('users/ollielballinger/Bogota_CoC_data')

task.start()
