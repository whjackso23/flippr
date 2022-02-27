import datetime as dt
from flippr.utils.handlers import spotify_handler
import os

config = {
    's3': {
        'bucket': 'flippr-data',
        'prefix': os.path.join('spotify', str(dt.date.today()))
    },
    'spotify': {
        'client_id': os.getenv('SPOTIFY_CLIENT_ID'),
        'client_secret': os.getenv('SPOTIFY_CLIENT_SECRET')
    }
}

if __name__ == '__main__':

    spotify_playlists = [
        {'title': 'top_50_global', 'id': '37i9dQZEVXbNG2KDcFcKOF'},
        {'title': 'country', 'id': '37i9dQZF1DX1lVhptIYRda'},
        {'title': 'rock', 'id': '37i9dQZF1DXcF6B6QPhFDv'},
        {'title': 'latin', 'id': '37i9dQZF1DX10zKzsJ2jva'},
        {'title': 'r&b', 'id': '37i9dQZF1DX4SBhb3fqCJd'},
        {'title': 'christian', 'id': '37i9dQZF1DXaod7SIWA11W'},
        {'title': 'folk', 'id': '37i9dQZF1DXaUDcU6KDCj4'},
        {'title': 'jazz', 'id': '37i9dQZF1DX7YCknf2jT6s'},
        {'title': 'rap', 'id': '37i9dQZF1DX0XUsuxWHRQd'},
        {'title': 'edm', 'id': '37i9dQZF1DX4dyzvuaRJ0n'},
        {'title': 'pollen', 'id': '37i9dQZF1DWWBHeXOYZf74'}
    ]

    spotify_handler(config, spotify_playlists)