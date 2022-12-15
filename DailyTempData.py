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
    open('temp.csv', 'wb').write(r.content)
    df = pd.read_csv('temp.csv')
    df = changMissingData(df)
    df = addDateInfor(df)
    df = df[['STATION', 'LATITUDE', 'LONGITUDE', 'NAME', 'YEAR', 'DATE','TEMP', 'MIN', 'MAX']]
    return df



def combineYearData(urls):
    df1 = pd.DataFrame()
    for url in urls:
        try:
            df2 = findDataFrame(url)
            df = pd.concat([df1, df2], axis = 0)
            df1 = df
        except:
            print('result has not been initialized')
    return df1


years = ['1990', '1991', '1992', '1993', '1994', '1995', '1996', '1997', '1998']


for year in years:
    basicUrl = 'https://www.ncei.noaa.gov/data/global-summary-of-the-day/access/'
    fileList = findFileNames(basicUrl, year)
    urls = createUrls(fileList, year)
    df = combineYearData(urls)
    df.to_csv('DailyTemp_' + year + '.csv')