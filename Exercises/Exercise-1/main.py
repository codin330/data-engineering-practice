import requests
import os
import zipfile

download_uris = [
    'https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip',
    'https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip',
    'https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip',
    'https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip',
    'https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip',
    'https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip',
    'https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip'
]

def get_file_name_from_uri(uri:str) -> str:
    ''' Extract file name from uri

    uri:str - uri
    '''
    l = uri.split(sep='/')
    return l[len(l)-1]

def get_csv_file_name(zip_file_name:str) -> str:
    ''' Get csv file name from zip file

    zip_file_name:str - zip file name
    '''
    file_name = zip_file_name.split(sep='.')[0] + '.csv'
    return file_name

def unzip_csv(zip_file_name:str):
    ''' Unzip csv and remove original zip file

    zip_file_name:str - name of zip file
    '''
    with zipfile.ZipFile(zip_file_name, 'r') as z:
        z.extract(get_csv_file_name(zip_file_name))
        print('CSV extracted successfully')
        z.close()
    os.remove(zip_file_name)

def download_zip(uri:str) -> bool:
    ''' download zip files from uris

    uri:str - uri of the zip file
    '''
    print(f'Downloading {uri}')
    ret = False
    try:
        r = requests.get(uri)
        if r.status_code == 200:
            print(f'Valid uri, File size={len(r.content)}')
            file_name = get_file_name_from_uri(uri)
            with open(file_name, mode='wb') as f:
                f.write(r.content)
                f.close()
            ret = True
        else:
            print(f'  Invalid uri, status_code={r.status_code})')
    except Exception as e:
        print(f'Error: {e}')

    return ret

def main():
    # create download subdirectory if not present and make it current
    if not 'downloads' in os.listdir('.'):
        os.makedirs('downloads')
    os.chdir('downloads')

    # download files from uris
    for uri in download_uris:
        if download_zip(uri):
            unzip_csv(get_file_name_from_uri(uri))

if __name__ == '__main__':
    main()
