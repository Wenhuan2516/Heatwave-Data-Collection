import pandas as pd
import numpy as np
import dask.dataframe as ddf
from pandas import Series, DataFrame
import seaborn as sn
import plotly.express as px

years = ['2010', '2011', '2012', '2013', '2014', '2015', '2016', '2017', '2018', '2019', '2020']
frames = []
for year in years:
    ur_files = ddf.read_csv('DailyTemp/DailyTemp_' + year + '.csv', dtype = {'fips': str, 'STATION': 'object'})
    temp = ur_files.compute()
    temp = temp.loc[:, ~temp.columns.str.contains('^Unnamed')]
    temp['year'] = year
    frames.append(temp)
    
temp = pd.concat(frames)


frames = []
for year in years:
    ur_files = ddf.read_csv('temp_station/temp_station_' + year + '.csv', dtype = {'STATEFP': str, 'COUNTYFP': str, 'TRACTCE':'object','station': str})
    temp_station = ur_files.compute()
    temp_station = temp_station.loc[:, ~temp_station.columns.str.contains('^Unnamed')]
    frames.append(temp_station)
    
temp_station = pd.concat(frames)

temp_station = temp_station.dropna()
temp_station['year'] = temp_station['year'].astype(int)
temp = temp.rename(columns = {'STATION': 'station'})
temp['year'] = temp['year'].astype(int)


for year in years:
    df_temp = temp[temp['year'] == year]
    df_station = temp_station[temp_station['year'] == year]
    df_station = df_station.merge(df_temp[['station', 'year', 'DATE', 'TEMP', 'MIN', 'MAX']], on = ['station', 'year'], how = 'left')
    df_station.to_csv('daily_temp_county_level_' + year + '.csv')