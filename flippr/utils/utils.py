import boto3
import os
import requests
import time

def write_file_to_s3(config, filename):

    """
    write a file to s3
    :param config: config for s3
    :param csv_filename: filename for local and remote
    :return:
    """
    s3 = boto3.client("s3",
                      aws_access_key_id=config['aws']['access_key'],
                      aws_secret_access_key=config['aws']['secret']
                      )
    s3.upload_file(filename, config['s3']['bucket'], os.path.join(config['s3']['prefix'], filename))

def df_to_s3(config, input_df, filename):

    """
    ingest a dataframe and a csv filename, write that dataframe to S3 with that filename
    :param config: config for s3
    :param input_df: dataframe to be converted to csv and sent to s3
    :param filename: filename to save df as and send to s3
    :return:
    """
    outfile = filename
    input_df.to_csv(outfile, index=False)
    print("Data written to {}".format(outfile))

    write_file_to_s3(config, outfile)
    os.remove(outfile)

def download_file_from_s3(config, filename):

    """

    :param config:
    :param filename:
    :return:
    """

    s3 = boto3.client("s3",
                      aws_access_key_id=config['aws']['access_key'],
                      aws_secret_access_key=config['aws']['secret']
                      )
    print(f"downloading {config['s3']['bucket']}/{os.path.join(config['s3']['prefix'])}/{filename}")
    s3.download_file(config['s3']['bucket'], os.path.join(config['s3']['prefix'], filename), filename)


def ticketmaster_keyword_request(string, keyword, keys, key_idx):

    """
    fucntion to request data from ticketmaster endpoint
    :param string:
    :param key:
    :return:
    """
    try:
        key = keys[key_idx]
    except IndexError:
        print('Out of keys! Make a new app in Ticketmaster')
        return None
    response = requests.get(string.format(keyword, key))
    if 'fault' in response.json():
        print('THERE IS A FAULT IN THE KEYWORD QUERY!!')
        print(response.json())
        if response.json()['fault']['detail']['errorcode'] == 'policies.ratelimit.SpikeArrestViolation':
            time.sleep(10)
            return ticketmaster_keyword_request(string, keyword, keys, key_idx)
        else:
            key_idx = key_idx+1
            print(f'new key_idx is {key_idx}')
            return ticketmaster_keyword_request(string, keyword, keys, key_idx)
    else:
        return response.json()

def ticketmaster_event_request(string, event, keys, key_idx):

    """
    fucntion to request data from ticketmaster endpoint
    :param string:
    :param key:
    :return:
    """
    try:
        key = keys[key_idx]
    except IndexError:
        print('Out of keys! Make a new app in Ticketmaster')
        return None
    response = requests.get(string.format(event['id'], key))
    if 'fault' in response.json():
        print('THERE IS A FAULT IN THE EVENT DETAIL QUERY!!')
        print(response.json())
        if response.json()['fault']['detail']['errorcode'] == 'policies.ratelimit.SpikeArrestViolation':
            print('Just waiting 10 seconds to avoid the ratelimit error')
            time.sleep(10)
            return ticketmaster_event_request(string, event, keys, key_idx)
        if response.json()['fault']['detail']['errorcode'] == 'messaging.adaptors.http.flow.GatewayTimeout':
            print('Gateway timeout, trying again')
            time.sleep(10)
            return ticketmaster_event_request(string, event, keys, key_idx)
        else:
            print('Switching key')
            key_idx = key_idx+1
            print(f'new key_idx is {key_idx}')
            return ticketmaster_event_request(string, event, keys, key_idx)
    else:
        return key_idx, response.json()