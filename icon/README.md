# ICON-weather
 
Very simple python script for parse ICON weather forecast and put it into mongodb database.
Tested only with Debian 11!

Don't forget to install:
cdo
libeccodes-tools
wget

Dont forget to install python libs:
xarray
ecmwflibs
pymongo
netCDF4

For data download you can use downloaddata.sh HH (where HH is ['00', '06', '12', '18'])
