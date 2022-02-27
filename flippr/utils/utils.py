import boto3
import os

def write_file_to_s3(config, filename):

    """
    write a file to s3
    :param config: config for s3
    :param csv_filename: filename for local and remote
    :return:
    """
    s3 = boto3.client("s3",
                      aws_access_key_id = config['aws']['access_key'],
                      aws_secret_access_key = config['aws']['secret']
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