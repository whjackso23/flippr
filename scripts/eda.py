import datetime as dt
from flippr.utils.eda import basic_eda, stubhub_eda
import os
import argparse

config = {
    'aws': {
        'access_key': os.getenv('AWS_ACCESS_KEY'),
        'secret': os.getenv('AWS_SECRET')
    },
    's3': {
        'aggregate_prefix': os.path.join('aggregate'),
        'spotify_prefix': os.path.join('spotify', str(dt.date.today())),
        'ticketmaster_prefix': os.path.join('ticketmaster', str(dt.date.today())),
        'seatgeek_prefix': os.path.join('seatgeek', str(dt.date.today())),
        'stubhub_prefix': os.path.join('stubhub', str(dt.date.today()))
    },
    'date': {
        'current': str(dt.date.today())
    },
}

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description="Collect daily ticket pricing data")
    parser.add_argument("--env", type=str, required=True)
    parser.add_argument('--sample', type=int, default=100)

    args = parser.parse_args()
    args_dict = vars(args)
    config['s3']['bucket'] = f"flippr-{args_dict['env']}"
    config['sample'] = args_dict['sample']

    # basic_eda(config)
    stubhub_eda(config)