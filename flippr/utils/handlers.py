import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from flippr.utils.utils import df_to_s3, download_file_from_s3
import pandas as pd
import time
import requests

def spotify_handler(config, playlists):

    """
    function to gather & store spotify data
    :param config:
    :param playlists:
    :return:
    """

    # change config to use the spotify prefix
    config['s3']['prefix'] = config['spotify_prefix']

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
    return track_df

def ticketmaster_handler(config):

    """

    :param config:
    :return:
    """

    # change config to use the ticketmaster prefix
    config['s3']['prefix']=config['s3']['ticketmaster_prefix']

    # Download the file from S3
    download_file_from_s3(config, 'spotify_artists.csv')
    artist_df = pd.read_csv('spotify_artists.csv')

    for _, row in artist_df.iterrows():

        artist = row['artist']
        keyword_response = requests.get(
            f"https://app.ticketmaster.com/discovery/v2/events.json?size=10&keyword={artist}&apikey=BWD804PIoCjc6qiOZsaeaqtWXGPCfF0t")
        res_json = keyword_response.json()['_embedded']
        search_df = pd.DataFrame()
        features = ['name', 'id']
        for event in res_json['events']:
            search_dict = {}
            for feature in features:
                search_dict[feature] = event[feature]

            _df = pd.DataFrame([search_dict])
            search_df = pd.concat([search_df, _df], axis=0)

        event_df = pd.DataFrame()
        for event_id in search_df['id']:

            event_response = requests.get(
                f"https://app.ticketmaster.com/discovery/v2/events/{event_id}.json?apikey=BWD804PIoCjc6qiOZsaeaqtWXGPCfF0t")
            res_json = event_response.json()
            #         features = ['name', 'id', 'type', 'min_price', 'max_price', 'start_date']

            event_dict = {}
            event_dict['name'] = res_json['name']
            event_dict['id'] = res_json['id']
            event_dict['start_date'] = res_json['dates']['start']['dateTime']

            for price_data in res_json['priceRanges']:

                if price_data['type'] == 'standard':
                    event_dict['type'] = price_data['type']
                    event_dict['min_price'] = price_data['min']
                    event_dict['max_price'] = price_data['max']

            _df = pd.DataFrame([event_dict])
            event_df = pd.concat([event_df, _df], axis=0)

            time.sleep(1)
    return event_df