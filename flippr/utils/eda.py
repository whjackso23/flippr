from flippr.utils.utils import df_to_s3, download_file_from_s3
import pandas as pd
import time
import requests
import botocore
import datetime as dt
import os
import json

def stubhub_eda(config):
    """
    validation on stubhub data
    """
    try:
        download_file_from_s3(config, 'stubhub', 'all_artists_upcoming_events.csv')
        _df = pd.read_csv('all_artists_upcoming_events.csv')
        print(_df.dtypes)
        print(_df.head(10))

    except botocore.exceptions.ClientError as err:
        print(err)

def basic_eda(config):
    """
    Just visualize some data
    """
    try:
        # Download the file from S3
        download_file_from_s3(config, 'aggregate', 'artists_events_prices.csv')
    except botocore.exceptions.ClientError:
        print(f"EDA job needs an aggregate file to run...and one doesn't appear to exist!")
        return
    events_df = pd.read_csv('artists_events_prices.csv')
    events_df['start_dt'] = events_df['start_date'].astype('datetime64[ns]')
    events_df = events_df[['ticketmaster_id', 'artist_keyword', 'venue_name', 'city', 'state', 'start_date', 'start_dt']].drop_duplicates()
    events_df = events_df[events_df['start_dt'] >= dt.datetime.now()]