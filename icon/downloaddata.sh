#!/bin/sh

STARTDIR=`pwd`
cd data
wget  --recursive --no-parent https://opendata.dwd.de/weather/nwp/icon/grib/$1/t_2m/
wget  --recursive --no-parent https://opendata.dwd.de/weather/nwp/icon/grib/$1/relhum_2m/
wget  --recursive --no-parent https://opendata.dwd.de/weather/nwp/icon/grib/$1/tot_prec/
wget  --recursive --no-parent https://opendata.dwd.de/weather/nwp/icon/grib/$1/u_10m/
wget  --recursive --no-parent https://opendata.dwd.de/weather/nwp/icon/grib/$1/v_10m/
wget  --recursive --no-parent https://opendata.dwd.de/weather/nwp/icon/grib/$1/vmax_10m/
wget  --recursive --no-parent https://opendata.dwd.de/weather/nwp/icon/grib/$1/td_2m/
wget  --recursive --no-parent https://opendata.dwd.de/weather/nwp/icon/grib/$1/h_snow/
cd ${STARTDIR}
