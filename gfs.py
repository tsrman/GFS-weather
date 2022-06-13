import sys
from urllib import request
import os
import datetime
import xarray as xr
import json
from pymongo import MongoClient


# constants
mongodb_url = 'mongodb://localhost:27017/'
mongodb_database = 'weather'
timetable = ['00', '06', '12', '18']
url_2m_above = 'https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl?file=gfs.t{}z.pgrb2.0p25.f{}&lev_2_m_above_ground=on&var_DPT=on&var_RH=on&var_TMP=on&leftlon=0&rightlon=360&toplat=90&bottomlat=-90&dir=%2Fgfs.{}%2F{}%2Fatmos'
url_10m_above = 'https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl?file=gfs.t{}z.pgrb2.0p25.f{}&lev_10_m_above_ground=on&var_UGRD=on&var_VGRD=on&leftlon=0&rightlon=360&toplat=90&bottomlat=-90&dir=%2Fgfs.{}%2F{}%2Fatmos'
url_surface = 'https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl?file=gfs.t{}z.pgrb2.0p25.f{}&lev_surface=on&var_GUST=on&var_PRATE=on&var_PRES=on&var_SNOD=on&var_TMP=on&leftlon=0&rightlon=360&toplat=90&bottomlat=-90&dir=%2Fgfs.{}%2F{}%2Fatmos'
tmp_gfs_2m = 'gfs_2m.grib'
tmp_gfs_10m = 'gfs_10m.grib'
tmp_gfs_sf = 'gfs_sf.grib'
remove_fields = ['valid_time', 'time', 'step', 'heightAboveGround', 'longitude', 'latitude']
kelvin_fields = ['t', 't2m', 'd2m']

def delete_temp_files(forecast):
    if os.path.exists(forecast + tmp_gfs_2m):
        os.remove(forecast + tmp_gfs_2m)
    if os.path.exists(forecast + tmp_gfs_10m):
        os.remove(forecast + tmp_gfs_10m)
    if os.path.exists(forecast + tmp_gfs_sf):
        os.remove(forecast + tmp_gfs_sf)

def download_data(date, hour, forecast):
    try:
        print('Download data...')
        url_gfs_2m = url_2m_above.format(hour, forecast, date, hour)
        url_gfs_10m = url_10m_above.format(hour, forecast, date, hour)
        url_gfs_sf = url_surface.format(hour, forecast, date, hour)
        request.urlretrieve(url_gfs_2m, forecast + tmp_gfs_2m)
        request.urlretrieve(url_gfs_10m, forecast + tmp_gfs_10m)
        request.urlretrieve(url_gfs_sf, forecast + tmp_gfs_sf)
        return True
    except BaseException as err:
        print(f"Unexpected {err=}, {type(err)=}")
        return False

def processing_data(foreacst):
    try:
        print('Process data...')
        ds = xr.load_dataset(foreacst + tmp_gfs_2m, engine="cfgrib")
        ds_tmp1 = xr.load_dataset(foreacst + tmp_gfs_10m, engine="cfgrib")
        ds_tmp2 = xr.load_dataset(foreacst + tmp_gfs_sf, engine="cfgrib", backend_kwargs={'filter_by_keys': {'stepType': 'instant'}})
        for v in ds:
            print("{}, {}, {}".format(v, ds[v].attrs["long_name"], ds[v].attrs["units"]))
        for v in ds_tmp1:
            print("{}, {}, {}".format(v, ds_tmp1[v].attrs["long_name"], ds_tmp1[v].attrs["units"]))
        for v in ds_tmp2:
            print("{}, {}, {}".format(v, ds_tmp2[v].attrs["long_name"], ds_tmp2[v].attrs["units"]))

        print('Compute data...')
        df = ds.to_dataframe()
        df_tmp = ds_tmp1.to_dataframe()
        df['u10'] = df_tmp['u10']
        df['v10'] = df_tmp['v10']
        df_tmp = ds_tmp2.to_dataframe()
        df['gust'] = df_tmp['gust']
        df['sp'] = df_tmp['sp']
        df['t'] = df_tmp['t']
        df['sde'] = df_tmp['sde']
        df['prate'] = df_tmp['prate']

        latitudes = df.index.get_level_values("latitude")
        longitudes = df.index.get_level_values("longitude")
        map_function = lambda lon: (lon - 360) if (lon > 180) else lon
        remapped_longitudes = longitudes.map(map_function)
        df["longitude"] = remapped_longitudes
        df["latitude"] = latitudes

        for i in kelvin_fields:
            df[i]=df[i]-273.15  
    
        records = json.loads(df.T.to_json()).values()
        ins = []
        for i in records:
            x_tmp = i
            x_tmp['loc'] = {'type': 'Point', 'coordinates': [ i['longitude'], i['latitude']] }
            x_tmp['date'] = datetime.datetime.utcfromtimestamp(i['valid_time']/1000.0)
            for j in remove_fields:
                del x_tmp[j]
            ins.append(x_tmp)

        client = MongoClient(mongodb_url)
        db = client[mongodb_database]
        mongo_coll = db['gfs_tmp']
        print('Start inserting...')
        mongo_coll.insert_many(ins)
        print('Creating indexes...')
        mongo_coll.create_index([('date', 1)])
        mongo_coll.create_index([('loc', '2dsphere')])
        delete_temp_files(foreacst)
        return True
    except BaseException as err:
        print(f"Unexpected {err=}, {type(err)=}")
        return False

def rename_collection():
    try:
        print('Rename collection')
        client = MongoClient(mongodb_url)
        db = client[mongodb_database]
        mongo_coll = db['gfs_tmp']
        mongo_coll.rename('gfs', dropTarget = True)
        return True
    except BaseException as err:
        print(f"Unexpected {err=}, {type(err)=}")
        return False

def main(argv):
    now = datetime.datetime.utcnow()
    datestr = now.strftime('%Y%m%d')
    current_hour = int(now.strftime('%H'))
    data_hour = timetable[int(current_hour / 6)]
    print('Processing: {} {}'.format(datestr, data_hour))
    range_start = 0 # test 0 - 381 with step 3
    range_end = 384 # test 3 - 384 with step 3

    for j in range(range_start, range_end + 1, 3):
        forecast = f'{j:03d}'
        print('Forecast #: {}'.format(forecast))
        if download_data(datestr, data_hour, forecast):
            processing_data(forecast)
    
    rename_collection()
    exit(0)

if __name__ == "__main__":
   main(sys.argv[1:])
