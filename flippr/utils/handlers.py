import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from flippr.utils.utils import df_to_s3, download_file_from_s3, ticketmaster_keyword_request, ticketmaster_event_request
import pandas as pd
import time
import requests
import botocore
import datetime as dt

def spotify_handler(config, playlists):

    """
    function to gather & store spotify data
    :param config:
    :param playlists:
    :return:
    """

    # change config to use the spotify prefix
    config['s3']['prefix'] = config['s3']['spotify_prefix']

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
                                     }])

                track_df = pd.concat([track_df, _df], axis=0)
            except TypeError:
                print(f'No data for this track in the playlist {target_playlist["title"]}')
        time.sleep(1)

    df_to_s3(config, track_df, 'spotify_artists.csv')
    print(f'Finished Spotify target artist file creation at {dt.datetime.now()}')
    return


def ticketmaster_handler(config):
    """

    :param config:
    :return:
    """

    # change config to use the spotify prefix
    config['s3']['prefix'] = config['s3']['spotify_prefix']

    # Download the file from S3
    download_file_from_s3(config, 'spotify_artists.csv')
    artist_df = pd.read_csv('spotify_artists.csv')

    # change config to use ticketmaster prefix
    config['s3']['prefix'] = config['s3']['ticketmaster_prefix']

    # initialize empty dataframe
    full_df = pd.DataFrame()

    # list to track which artists have already been queried
    artist_list = []

    key_idx = 0
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
            print('No events for this artist in Ticketmaster')
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
                df_to_s3(config, full_df, 'all_artists_upcoming_events.csv')
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
                event_dict['id'] = res_json['id']
                event_dict['start_date'] = res_json['dates']['start']['localDate']
                event_dict['city'] = res_json['_embedded']['venues'][0]['city']['name']
                event_dict['job_date'] = config['date']['current']

                # handle foreign events
                try:
                    event_dict['state'] = res_json['_embedded']['venues'][0]['state']['name']
                except KeyError:
                    event_dict['state'] = ''

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
    df_to_s3(config, full_df, 'all_artists_upcoming_events.csv')
    print(f'Finished Ticketmaster target artist event details file creation at {dt.datetime.now()}')
    return


def cross_platform_event_df(config, platforms):

    """
    maintain a common file containing all price info for every event from every available platform

    :return:
    """

    for platform in platforms:

        # get most recent daily run of all artist upcoming events
        config['s3']['prefix'] = config['s3'][f'{platform}_prefix']
        _filename = 'all_artists_upcoming_events.csv'
        download_file_from_s3(config, _filename)
        _df = pd.read_csv(_filename)

        # change config to use aggregate prefix
        config['s3']['prefix'] = config['s3']['agg_prefix']
        _filename = 'artists_events_prices.csv'

        # check if the aggregate file exists at all yet, create it if not
        try:
            download_file_from_s3(config, _filename)
            agg_df = pd.read_csv(_filename)
        except botocore.exceptions.ClientError:
            print('File doesnt exist, creating it now!')
            agg_df = pd.DataFrame()

        # append today's data to master aggregate df
        agg_df = pd.concat([agg_df, _df], axis=0)
        print(f'Finished multi-source master file creation at {dt.datetime.now()}')
        df_to_s3(config, agg_df, 'artists_events_prices.csv')
        return