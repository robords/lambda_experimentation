#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import boto3
import datetime
from io import StringIO


def get_and_put_data_from_noaa():
    '''
    The NOAA data is organized into yearly files, so all I need to do is overwrite the file
    for the year where we're trying to fill in the gaps
    http://noaa-ghcn-pds.s3.amazonaws.com/csv/1788.csv
    '''
    bucket = 'noaa-ghcn-pds'
    path_name = 'csv.gz'
    
    current_year = datetime.datetime.now().year
    s3 = boto3.client('s3')
    response = s3.list_objects_v2(
            Bucket=bucket,
            Prefix=path_name 
        )
    print("got keys")
    key = [i['Key']  for i in response['Contents'] if f'/{str(current_year)}.csv.gz' in i['Key']]
    response = s3.get_object(Bucket=bucket, Key=key[0])
    print("got response")
    most_recent_df = pd.read_csv(response.get("Body"), compression='gzip', 
                                 names=['id','date-','element','r-eported_value',
                                        'M-FLAG','Q-FLAG','S-FLAG','OBS-TIME'])
    print("read NOAA file")
    # filter out the columns with bad data
    most_recent_df =  most_recent_df[~(most_recent_df['Q-FLAG'].isnull())]
    # combine the file with the stations file to get the states
    response = s3.get_object(Bucket='raw-weather-data', Key='ghcnd-stations.csv')
    print("got stations response")
    stations = pd.read_csv(response.get("Body"))
    print("read stations file")
    most_recent_df = most_recent_df.merge(stations, left_on='id', right_on='id')
    most_recent_df.rename(columns={"state": 'location'}, inplace=True)
    # create a separate file for each value in the third column
    elements = ['PRCP','SNOW','SNWD','TMAX','TMIN']
    for i in elements:
        # filter the dataframe for just the records with that element
        df = most_recent_df[(most_recent_df.element == i)]
        # select only the columns we need:
        df = df[['date-', 'r-eported_value', 'location']]
        # update the date field so it's got dashes
        df['date-'] = pd.to_datetime(df['date-'], format='%Y%m%d') 
        df['date-'] = df['date-'].dt.strftime('%Y-%m-%d')
        
        # write it directly to s3
        bucket = 'raw-weather-data'
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        s3_resource = boto3.resource('s3')
        s3_resource.Object(bucket, f'{i}/{current_year}.csv').put(Body=csv_buffer.getvalue())
        # This should release it from memory
        del df

get_and_put_data_from_noaa()




