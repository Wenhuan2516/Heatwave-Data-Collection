import pandas as pd
import numpy as np
import dask.dataframe as ddf
from pandas import Series, DataFrame
import requests
from bs4 import BeautifulSoup
from bs4 import Tag
import re
import csv
import json
import datetime

def generate_soup(url):
    response = requests.get(url)
    html = response.text
    return BeautifulSoup(html, "html.parser")

def findFileNames(basicUrl, year):
    url_year = basicUrl + year + '/'
    soup = generate_soup(url_year)
    files = soup.findAll('td')
    fileNames = []
    for file in files[1:]:
        if file.find('a') != None:
            fileName = file.find('a').text
            fileNames.append(fileName)
    return fileNames

def changeToNone(value):
    if value == 999.9 or value == 9999.9:
        return np.nan
    else:
        return value
    
def changeToNone3(value):
    if value == 99.99:
        return np.nan
    else:
        return value
    

def changMissingData(df):
    df['TEMP'] = df['TEMP'].apply(changeToNone)   # missing data = 9999.9
    df['DEWP'] = df['DEWP'].apply(changeToNone)   # missing data = 9999.9
    df['SLP'] = df['SLP'].apply(changeToNone)     # missing data = 9999.9
    df['STP'] = df['STP'].apply(changeToNone)     # missing data = 9999.9
    df['STP'] = df['STP'].apply(changeToNone)     # missing data = 999.9
    df['VISIB'] = df['VISIB'].apply(changeToNone) # missing data = 999.9
    df['WDSP'] = df['WDSP'].apply(changeToNone)   # missing data = 999.9
    df['MXSPD'] = df['MXSPD'].apply(changeToNone) # missing data = 999.9
    df['GUST'] = df['GUST'].apply(changeToNone)   # missing data = 999.9
    df['MAX'] = df['MAX'].apply(changeToNone)     # missing data = 9999.9
    df['MIN'] = df['MIN'].apply(changeToNone)     # missing data = 9999.9
    df['PRCP'] = df['PRCP'].apply(changeToNone3)   # missing data = 99.99
    df['SNDP'] = df['SNDP'].apply(changeToNone)   # missing data = 999.9
    return df


def addDateInfor(df):
    df['DATE'] = pd.to_datetime(df['DATE'])
    df['YEAR'] = df['DATE'].dt.year
    return df


def createUrls(fileList, year):
    urls = []
    for element in fileList:
        url = "https://www.ncei.noaa.gov/data/global-summary-of-the-day/access/" + year + "/" + element
        urls.append(url)
    return urls

def findDataFrame(url):
    r = requests.get(url)
    open('temp2.csv', 'wb').write(r.content)
    df = pd.read_csv('temp2.csv')
    df = changMissingData(df)
    df = addDateInfor(df)
    return df

def groupByYear(df):
    if df.empty == False:
        df_year = df.groupby(['STATION', 'LATITUDE', 'LONGITUDE', 'NAME', 'YEAR'], as_index=False).agg(max)
        df_year = df_year.reset_index()
        df_year = df_year[['STATION', 'LATITUDE', 'LONGITUDE', 'NAME', 'YEAR', 'TEMP', 'MIN', 'MAX']]
        return df_year

def yearTemp(url):
    df = findDataFrame(url)
    df_year = groupByYear(df)
    return df_year


def combineYearData(urls):
    df1 = pd.DataFrame()
    for url in urls:
        try:
            df2 = yearTemp(url)
            df = pd.concat([df1, df2], axis = 0)
            df1 = df
        except:
            print('result has not been initialized')
    return df1


years = ['1961', '1962', '1963', '1964', '1965', '1966', '1967', '1968', '1969', '1970',
         '1971', '1972', '1973', '1974', '1975', '1976', '1977', '1978', '1979', '1980',
         '1981', '1982', '1983', '1984', '1985', '1986', '1987', '1988', '1989', '1990']


for year in years:
    basicUrl = 'https://www.ncei.noaa.gov/data/global-summary-of-the-day/access/'
    fileList = findFileNames(basicUrl, year)
    urls = createUrls(fileList, year)
    df = combineYearData(urls)
    df.to_csv('MaxTemp_' + year + '.csv')
    