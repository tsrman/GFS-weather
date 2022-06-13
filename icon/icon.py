import os
import sys
import xarray as xr
import json
from pymongo import MongoClient
import datetime

# constants
mongodb_url = 'mongodb://localhost:27017/'
mongodb_database = 'weather'
base_dir = '/home/tsr/'
data_dir = 'data/'
work_dir = 'out/'
opendata_prefix = 'opendata.dwd.de/weather/nwp/icon/grib/'
table_dirs = ['t_2m', 'relhum_2m', 'tot_prec', 'u_10m', 'v_10m', 'vmax_10m', 'td_2m', 'h_snow']
timetable = ['00', '06', '12', '18']
forecasttable = []
f_file_name = {}
f_file_name['h_snow'] = 'icon_global_icosahedral_single-level_{}{}_{}_H_SNOW.grib2'
f_file_name['relhum_2m'] = 'icon_global_icosahedral_single-level_{}{}_{}_RELHUM_2M.grib2'
f_file_name['t_2m'] = 'icon_global_icosahedral_single-level_{}{}_{}_T_2M.grib2'
f_file_name['td_2m'] = 'icon_global_icosahedral_single-level_{}{}_{}_TD_2M.grib2'
f_file_name['tot_prec'] = 'icon_global_icosahedral_single-level_{}{}_{}_TOT_PREC.grib2'
f_file_name['u_10m'] = 'icon_global_icosahedral_single-level_{}{}_{}_U_10M.grib2'
f_file_name['v_10m'] = 'icon_global_icosahedral_single-level_{}{}_{}_V_10M.grib2'
f_file_name['vmax_10m'] = 'icon_global_icosahedral_single-level_{}{}_{}_VMAX_10M.grib2'
kelvin_fields = ['2t', '2d']
remove_fields = ['valid_time', 'longitude', 'latitude']

def unpack_data(hour):
    for i in table_dirs:
        path = '{}{}{}{}/{}/*.bz2'.format(base_dir, data_dir, opendata_prefix, hour, i)
        os.system('bzip2 -d {}'.format(path))

def compose_grib(date, hour):
    for i in forecasttable:
        out_path_tmp = base_dir + work_dir + '{}-{}-{}.tmp'.format(date, hour, i)
        out_path = base_dir + work_dir + '{}-{}-{}.nc'.format(date, hour, i)
        in_path = ''
        for j in table_dirs:
            in_path = in_path + '{}{}{}{}/{}/'.format(base_dir, data_dir, opendata_prefix, hour, j) + f_file_name[j].format(date, hour, i) + ' '
        os.system('grib_copy {} {}'.format(in_path, out_path_tmp))
        print(in_path)
        print(out_path_tmp)
        os.system('cdo -f nc remap,target_grid_world_025.txt,weights_icogl2world_025.nc {} {}'.format(out_path_tmp, out_path))
        os.remove(out_path_tmp)

def make_forecasttable():
    global forecasttable
    #forecasttable = ['177','180']
    #return
    for i in range(1,79):
        forecasttable.append(f'{i:03d}')
    #return
    for i in range(81,181, 3):
        forecasttable.append(f'{i:03d}')

def processing_data(date, data_hour):
    try:
        print('Process data...')
        for i in forecasttable:
            nc_file = '{}{}{}-{}-{}.nc'.format(base_dir, work_dir, date, data_hour, i)
            ds = xr.load_dataset(nc_file)
            for v in ds:
                print('{}, {}, {}'.format(v, ds[v].attrs['long_name'], ds[v].attrs['units']))
            print('Compute data...')
            
            df = ds.to_dataframe()
            latitudes = df.index.get_level_values('lat')
            longitudes = df.index.get_level_values('lon')
            valid_time = df.index.get_level_values('time')
            map_function = lambda lon: (lon - 360) if (lon > 180) else lon
            remapped_longitudes = longitudes.map(map_function)
            df['longitude'] = remapped_longitudes
            df['latitude'] = latitudes
            df['valid_time'] = valid_time
            for i in kelvin_fields:
                df[i]=df[i]-273.15  

            #df = df.head(1) # for debug
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
            mongo_coll = db['icon_tmp']
            print('Start inserting...')
            mongo_coll.insert_many(ins)
            print('Creating indexes...')
            mongo_coll.create_index([('date', 1)])
            mongo_coll.create_index([('loc', '2dsphere')])
        return True
    except BaseException as err:
        print(f"Unexpected {err=}, {type(err)=}")
        return False    

def rename_collection():
    try:
        print('Rename collection')
        client = MongoClient(mongodb_url)
        db = client[mongodb_database]
        mongo_coll = db['icon_tmp']
        mongo_coll.rename('icon', dropTarget = True)
        return True
    except BaseException as err:
        print(f"Unexpected {err=}, {type(err)=}")
        return False

def main(argv):
    make_forecasttable()
    now = datetime.datetime.utcnow()
    datestr = now.strftime('%Y%m%d')
    current_hour = int(now.strftime('%H'))
    current_hour = 0 # for test
    data_hour = timetable[int(current_hour / 6)]
    unpack_data(data_hour)
    #datestr = '20220611' # for debug
    compose_grib(datestr, data_hour)
    processing_data(datestr, data_hour)
    rename_collection()

if __name__ == '__main__':
   main(sys.argv[1:])
