import requests
import os
import zipfile
import asyncio
import aiohttp
import aiofiles
import time

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
        csv_file_name = get_csv_file_name(zip_file_name)
        z.extract(csv_file_name)
        print(f'CSV extracted successfully {csv_file_name}')
        z.close()
    os.remove(zip_file_name)

async def download_zip(uri:str, session:aiohttp.ClientSession) -> None:
    ''' download zip files from uris

    uri:str - uri of the zip file
    session:aiohttp.ClientSession
    '''

    ret = False
    print(f'Downloading {uri}')
    try:
        r = await session.get(uri)
        r.raise_for_status()
        print(f'Valid uri {uri}')
        file_name = get_file_name_from_uri(uri)
        async with aiofiles.open(file_name, mode='wb') as f:
            await f.write(await r.content.read())
        unzip_csv(get_file_name_from_uri(uri))
        ret = True
    except Exception as e:
        print(f'Error: {e}')

    # return ret

async def main():

    start_time = time.time()
    print(f'Start time {time.ctime(start_time)}')

    # create download subdirectory if not present and make it current
    if not 'downloads' in os.listdir('.'):
        os.makedirs('downloads')
    os.chdir('downloads')

    # download files from uris
    async with aiohttp.ClientSession() as session:
        tasks = []
        for uri in download_uris:
            tasks.append(download_zip(uri, session))
        await asyncio.gather(*tasks)

    end_time = time.time()
    print(f'End time {time.ctime(end_time)}')
    print(f'Elapsed time {end_time - start_time:.2f} seconds')

if __name__ == '__main__':
    asyncio.run(main())
