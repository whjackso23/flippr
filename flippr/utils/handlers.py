import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from flippr.utils.utils import df_to_s3, download_file_from_s3, ticketmaster_keyword_request, ticketmaster_event_request, get_stubhub_keys
import pandas as pd
import time
import requests
import botocore
import datetime as dt
import os
import json

def spotify_handler(config, playlists):

    """
    function to gather & store spotify data
    :param config:
    :param playlists:
    :return:
    """

    client_credentials_manager = SpotifyClientCredentials(config['spotify']['client_id'], config['spotify']['client_secret'])
    sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)
    track_df = pd.DataFrame()
    for target_playlist in playlists:
        sp_playlist = sp.user_playlist('spotify', target_playlist['id'])
        for item in sp_playlist['tracks']['items']:
            track = item['track']
            try:
                artist_data = sp.artist(track['artists'][0]['id'])
                _df = pd.DataFrame([{'artist': track['artists'][0]['name']
                                        , 'id': track['artists'][0]['id']
                                        , 'artist_followers': artist_data['followers']['total']
                                        , 'artist_popularity': artist_data['popularity']
                                        , 'track_popularity': track['popularity']
                                        , 'playlist': target_playlist['title']
                                        , 'image_url': artist_data['images'][0]['url']
                                     }])
                track_df = pd.concat([track_df, _df], axis=0)
            except TypeError:
                print(f'No data for this track in the playlist {target_playlist["title"]}')
        time.sleep(1)
    df_to_s3(config, 'spotify', track_df, 'spotify_artists.csv')
    print(f'Finished Spotify target artist file creation at {dt.datetime.now()}')
    return

def ticketmaster_handler(config):
    """

    :param config:
    :return:
    """
    # Download the file from S3
    download_file_from_s3(config, 'spotify', 'spotify_artists.csv')
    artist_df = pd.read_csv('spotify_artists.csv')

    if config['sample'] <100:
        print('Only taking a sample')
        artist_df = artist_df.head(config['sample'])

    # initialize empty dataframe
    full_df = pd.DataFrame()
    # list to track which artists have already been queried
    artist_list = []
    no_events_list = []
    key_idx = 0
    print(f"Beginning Ticketmaster data pull at {dt.datetime.now()}")
    for _, row in artist_df.iterrows():
        artist = row['artist']
        exist_ind = artist_list.count(artist)
        if exist_ind > 0:
            print(f'Already queried the artist {artist}')
            continue
        artist_list.append(artist)
        req_string = """https://app.ticketmaster.com/discovery/v2/events.json?size=25&keyword={0}&sort=date,asc&apikey={1}"""
        res_json = ticketmaster_keyword_request(req_string, artist, config['ticketmaster']['keys'], key_idx)
        if res_json is None:
            print('Request quota exceeded')
            return
        try:
            res_json = res_json['_embedded']
        except KeyError:
            print(f'No events for artist {artist} in Ticketmaster')
            no_events_list.append(artist)
            continue
        search_df = pd.DataFrame()
        features = ['name', 'id']
        for event in res_json['events']:
            search_dict = {}
            search_dict['artist_keyword'] = artist
            for feature in features:
                search_dict[feature] = event[feature]
            _df = pd.DataFrame([search_dict])
            search_df = pd.concat([search_df, _df], axis=0)
        event_df = pd.DataFrame()
        for _, event in search_df.iterrows():
            # format event to be queried and execute query
            event = dict(event)
            req_string = """https://app.ticketmaster.com/discovery/v2/events/{0}.json?apikey={1}"""
            key_idx, res_json = ticketmaster_event_request(req_string, event, config['ticketmaster']['keys'], key_idx)
            if res_json is None:
                print('Request quota exceeded')
                # sort values
                event_df = event_df.sort_values(by=['start_date'])
                # add each artists' upcoming show ticket price ranges to full df
                full_df = pd.concat([full_df, event_df], axis=0)
                df_to_s3(config, 'ticketmaster', full_df, 'all_artists_upcoming_events.csv')
                print('Aborted early due to query quota limit violations.. :(')
                return
            # instantiate empty event dict to hold event data
            event_dict = {}

            # fill event dict with fields
            try:
                # critical fields
                event_dict['source'] = 'ticketmaster'
                event_dict['artist_keyword'] = event['artist_keyword']
                event_dict['name'] = res_json['name']
                event_dict['ticketmaster_id'] = res_json['id']
                event_dict['city'] = res_json['_embedded']['venues'][0]['city']['name']
                event_dict['venue_name'] = res_json['_embedded']['venues'][0]['name']
                event_dict['job_date'] = config['date']['current']

                # try to get event start date
                try:
                    event_dict['start_date'] = res_json['dates']['start']['dateTime']
                except KeyError:
                    event_dict['start_date'] = ''

                # handle foreign events
                try:
                    event_dict['state'] = res_json['_embedded']['venues'][0]['state']['stateCode']
                except KeyError:
                    event_dict['state'] = ''

                # try to get sales start / end date data
                try:
                    event_dict['public_sale_start_date'] = res_json['sales']['public']['startDateTime']
                    event_dict['public_sale_end_date'] = res_json['sales']['public']['endDateTime']
                    event_dict['pending_ind'] = res_json['sales']['public']['startTBD']
                except KeyError:
                    event_dict['public_sale_start_date'] = ''
                    event_dict['public_sale_end_date'] = ''
                    event_dict['pending_ind'] = ''

                # try to get presale info
                try:
                    event_dict['presale_start_date'] = res_json['sales']['presales'][0]['startDateTime']
                    event_dict['presale_end_date'] = res_json['sales']['presales'][0]['endDateTime']
                    event_dict['presale_link'] = res_json['sales']['presalse']['url']
                except KeyError:
                    event_dict['presale_start_date'] = ''
                    event_dict['presale_end_date'] = ''
                    event_dict['presale_link'] = ''

                # try to get price data, fall back to empty if none exist
                try:
                    for price_data in res_json['priceRanges']:
                        if price_data['type'] == 'standard':
                            event_dict['type'] = price_data['type']
                            event_dict['min_price'] = price_data['min']
                            event_dict['max_price'] = price_data['max']
                except KeyError:
                    event_dict['type'] = ''
                    event_dict['min_price'] = ''
                    event_dict['max_price'] = ''
                # try to get external social media links, fall back to empty if none exist
                try:
                    event_dict['instagram'] = (res_json['_embedded']['attractions'][0]['externalLinks']['instagram'][0]['url']).split('/')[-1]
                    _df = pd.DataFrame([event_dict])
                    event_df = pd.concat([event_df, _df], axis=0)
                except KeyError:
                    event_dict['instagram'] = ''
                    _df = pd.DataFrame([event_dict])
                    event_df = pd.concat([event_df, _df], axis=0)
            except KeyError:
                print('Something crucial missing from the json...here it is')
                print(res_json)
                continue
            # delay one second to avoid rate limits
            time.sleep(1)
        # only sort if there are any events with decent data at all
        if len(event_df) != 0:
            # sort values
            event_df = event_df.sort_values(by=['start_date'])
        # add each artists' upcoming show ticket price ranges to full df
        full_df = pd.concat([full_df, event_df], axis=0)
    # write full dataframe to s3
    df_to_s3(config, 'ticketmaster', full_df, 'all_artists_upcoming_events.csv')
    print(f'Finished Ticketmaster target artist event details file creation at {dt.datetime.now()}')
    return

def seatgeek_handler(config):

    """
    gather seatgeek data
    :param config:
    :return:
    """

    try:
        # Download the file from S3
        download_file_from_s3(config, 'aggregate', 'artists_events_prices.csv')
    except botocore.exceptions.ClientError:
        print('Seatgeek needs an aggregate file to run...try again tomorrow once Ticketmaster finishes!')
        return
    events_df = pd.read_csv('artists_events_prices.csv')
    events_df['start_dt'] = events_df['start_date'].astype('datetime64[ns]')
    events_df = events_df[['ticketmaster_id', 'artist_keyword', 'city', 'state', 'start_date', 'start_dt']].drop_duplicates()
    events_df = events_df[events_df['start_dt'] >= dt.datetime.now()]

    if config['sample'] < 100:
        print('Only taking a sample')
        events_df = events_df.head(config['sample'])

    # initialize empty dataframe
    full_df = pd.DataFrame()
    print(f'Beginning seatgeek data pull at {dt.datetime.now()}')
    for _, row in events_df.iterrows():
        artist_keyword = str(row['artist_keyword']).replace(' ', '-').replace('&', '')
        event_date = row['start_date']
        response = requests.get(
            f"https://api.seatgeek.com/2/events?client_id={config['seatgeek']['key']}&performers.slug={artist_keyword}&datetime_utc={event_date}")
        if response.status_code != 200:
            print(response)
            print(response.status_code)
            print(f"response status is not 200...somethings wrong!")
            print('')
            continue
        try:
            query_response = response.json()
        except json.decoder.JSONDecodeError:
            print('JSON ERROR ON QUERY')
            continue
        try:
            for event in query_response['events']:
                event_dict = {}
                event_response = requests.get(
                    f"https://api.seatgeek.com/2/events/{event['id']}?client_id={config['seatgeek']['key']}")
                if response.status_code != 200:
                    print(response)
                    print(response.status_code)
                    print(f"response status is not 200...somethings wrong!")
                    print('')
                    continue
                try:
                    event_response_data = event_response.json()
                except json.decoder.JSONDecodeError:
                    print('JSON ERROR ON EVENT')
                    continue
                event_dict['job_date'] = config['date']['current']
                event_dict['ticketmaster_id'] = row['ticketmaster_id']
                event_dict['seatgeek_id'] = event['id']
                event_dict['source'] = 'seatgeek'
                event_dict['artist_keyword'] = row['artist_keyword']
                event_dict['name'] = event_response_data['title']
                event_dict['start_date'] = row['start_date']
                event_dict['city'] = event_response_data['venue']['city']
                event_dict['state'] = event_response_data['venue']['state']
                event_dict['venue_name'] = event_response_data['venue']['name']
                event_dict['min_price'] = event_response_data['stats']['lowest_price']
                event_dict['max_price'] = event_response_data['stats']['highest_price']
                event_dict['ticket_count'] = event_response_data['stats']['listing_count']
                event_dict['average_price'] = event_response_data['stats']['average_price']
                event_dict['median_price'] = event_response_data['stats']['median_price']
                event_dict['ticket_bucket_counts'] = event_response_data['stats']['dq_bucket_counts']
                _df = pd.DataFrame([event_dict])
                full_df = pd.concat([full_df, _df], axis=0)
        except KeyError:
            print('')
            print(f"Event {artist_keyword} doesnt exist in Seatgeek :/")
    # write full dataframe to s3
    df_to_s3(config, 'seatgeek', full_df, 'all_artists_upcoming_events.csv')
    return full_df

def stubhub_handler(config):

    """
    get data from stubhub events

    :return:
    """

    get_stubhub_keys()


def cross_platform_event_df(config, platforms):

    """
    maintain a common file containing all price info for every event from every available platform

    :return:
    """

    # change config to use aggregate prefix
    config['s3']['today_aggregate_prefix'] = os.path.join(config['s3']['aggregate_prefix'], str(dt.date.today()))
    _filename = 'artists_events_prices.csv'

    # check if the aggregate job has already been run. If so return
    try:
        download_file_from_s3(config, 'today_aggregate', _filename)
        print('Aggregate job has already been run!')
        return
    # if this triggers then the job hasn't been run and we can proceed!
    except botocore.exceptions.ClientError:
        # check if the aggregate file exists at all yet, create it if not
        try:
            download_file_from_s3(config, 'aggregate', _filename)
            agg_df = pd.read_csv(_filename)
        except botocore.exceptions.ClientError:
            print('File doesnt exist, creating it now!')
            agg_df = pd.DataFrame()

        # now download each platforms' results from today and append them to the aggregate df
        for platform in platforms:
            _filename = 'all_artists_upcoming_events.csv'
            try:
                download_file_from_s3(config, platform, _filename)
                _df = pd.read_csv(_filename)
            except botocore.exceptions.ClientError:
                print(f"File {_filename} not found!")
                continue

            # append today's data to master aggregate df
            agg_df = pd.concat([agg_df, _df], axis=0)

        # now store the aggregate and "today" aggregate files in s3
        df_to_s3(config, 'aggregate', agg_df, 'artists_events_prices.csv')
        df_to_s3(config, 'today_aggregate', agg_df, 'artists_events_prices.csv')
        print(f'Finished multi-source master file creation at {dt.datetime.now()}')

    return