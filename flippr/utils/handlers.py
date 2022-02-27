import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import pandas as pd

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
    return track_df

chart_genre_ids = [
    {'title' :'top_50_global', 'id': '37i9dQZEVXbNG2KDcFcKOF'},
    {'title' :'country', 'id': '37i9dQZF1DX1lVhptIYRda'},
    {'title' :'rock', 'id': '37i9dQZF1DXcF6B6QPhFDv'},
    {'title' :'latin', 'id': '37i9dQZF1DX10zKzsJ2jva'},
    {'title' :'r&b', 'id': '37i9dQZF1DX4SBhb3fqCJd'},
    {'title' :'christian', 'id': '37i9dQZF1DXaod7SIWA11W'},
    {'title' :'folk', 'id': '37i9dQZF1DXaUDcU6KDCj4'},
    {'title' :'jazz', 'id': '37i9dQZF1DX7YCknf2jT6s'},
    {'title' :'rap', 'id': '37i9dQZF1DX0XUsuxWHRQd'},
    {'title' :'edm', 'id': '37i9dQZF1DX4dyzvuaRJ0n'},
    {'title' :'pollen', 'id': '37i9dQZF1DWWBHeXOYZf74'}
]
target_playlist_df = getTrackIDs(chart_genre_ids)
df_to_s3(config, target_playlist_df, 'spotify_artists.csv')