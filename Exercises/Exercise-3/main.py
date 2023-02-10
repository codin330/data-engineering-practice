import boto3
import zipfile
import os
import json
import gzip

# Using AWS credentials via environment variables
# AWS_ACCESS_KEY_ID - The access key for your AWS account.
# AWS_SECRET_ACCESS_KEY - The secret key for your AWS account.

bucket_name = 'commoncrawl'
bucket_object_key = 'crawl-data/CC-MAIN-2022-05/wet.paths.gz'

def set_download_dir() -> None:
    '''Check and if necessary creates downloads dir 

    '''

    # create download subdirectory if not present and make it current
    if not 'downloads' in os.listdir('.'):
        os.makedirs('downloads')
    os.chdir('downloads')

def get_s3_session() -> boto3.Session:
    ''' Return AWS s3 session. 
    
    AWS credentias should be defined in the configuration file app_teste_accessKeys.json
    '''

    # Get AWS credentials and create s3 session
    with open('app_teste_accessKeys.json','r') as f:
        j = json.load(f)
        f.close()

    session = boto3.Session(
        aws_access_key_id = j['aws_access_key_id'],
        aws_secret_access_key=j['aws_secret_access_key']
    )

    return session.resource('s3')

def main():

    s3 = get_s3_session()
    bucket = s3.Bucket(bucket_name)

    # Set download directorly
    set_download_dir()

    # Download uris file from s3 bucket
    gz_file_name = bucket_object_key[bucket_object_key.rindex('/')+1:len(bucket_object_key)]
    with open(gz_file_name,'ab') as f:
        bucket.download_file(bucket_object_key, gz_file_name)
        f.close()

    # Open gz and get uri to download
    text = ''
    with gzip.GzipFile(gz_file_name, 'r') as z:
        text = z.read()
        z.close()
    uri = ((str(text, 'UTF-8')).splitlines())[0]

    # Download file from uri
    file_name = uri[uri.rindex('/')+1:len(uri)]
    with open(file_name,'ab') as f:
        bucket.download_file(uri, file_name)
        f.close()

    # Open file and print lines in the stdout
    text = ''
    with gzip.GzipFile(file_name, 'r') as z:
        text = z.read()
        z.close()

    # lines = (str(text, 'UTF-8')).splitlines()
    # for n in range(0, 5):
    #     print(lines[n])

    for line in (str(text, 'UTF-8')).splitlines():
        print(line)

if __name__ == '__main__':
    main()
