import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from flippr.utils.utils import df_to_s3
import pandas as pd
import time

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
                                     }])

                track_df = pd.concat([track_df, _df], axis=0)
            except TypeError:
                print(f'No data for this track in the playlist {target_playlist["title"]}')
        time.sleep(1)

    df_to_s3(config, track_df, 'spotify_artists.csv')
    return track_df