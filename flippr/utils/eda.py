from flippr.utils.utils import df_to_s3, download_file_from_s3, ticketmaster_keyword_request, ticketmaster_event_request, get_stubhub_keys, stubhub_event_request
import pandas as pd
import time
import requests
import botocore
import datetime as dt
import os
import json

def basic_eda(config):
    """

    """
    try:
        # Download the file from S3
        download_file_from_s3(config, 'aggregate', 'artists_events_prices.csv')
    except botocore.exceptions.ClientError:
        print('Seatgeek needs an aggregate file to run...try again tomorrow once Ticketmaster finishes!')
        return
    events_df = pd.read_csv('artists_events_prices.csv')
    print(events_df.dtypes)
    events_df['start_dt'] = events_df['start_date'].astype('datetime64[ns]')
    events_df = events_df[['ticketmaster_id', 'artist_keyword', 'venue_name', 'city', 'state', 'start_date', 'start_dt']].drop_duplicates()
    events_df = events_df[events_df['start_dt'] >= dt.datetime.now()]

    test = events_df[events_df['artist_keyword'] == 'The Kid LAROI']
    print(test[['artist_keyword', 'city', 'state', 'venue_name', 'start_date']])