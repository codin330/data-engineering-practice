import requests
import pandas as pd
import os
from bs4 import BeautifulSoup
import re

gov_url = 'https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/'
target_date = '2022-02-07 14:03'

def get_csv_path(html:str, last_modified_date:str) -> tuple:
    '''
    Get path for csv file for the last modified date supplie.

    html:str - page HTML to extract info from
    last_modified_date:str - last modified date in the format 'yyyy-mm-dd hh:mm'
    '''

    bs = BeautifulSoup(html, 'html.parser')
    parent_directory = str(bs.head.title.string).split(sep=' ')[2]

    trs = bs.find_all('tr')
    file_name = None
    for tr in trs:
        tds = tr.find_all('td')
        if len(tds) > 0:
            if str(tds[1].string).strip() == last_modified_date:
                file_name = tds[0].a['href']
                break
    
    if parent_directory and file_name:
        return (parent_directory + '/' + file_name, file_name)
    else:
        return None

def get_html(url:str) -> str:
    '''
    Fetch HTML and return its content

    '''

    try:
        resp = requests.get(url)
        resp.raise_for_status()
        return resp.text
    except Exception as e:
        print(f'Exception {e}')
        return None

def download_file(uri:str, output_file_name:str) -> bool:
    ''' download file from uri

    uri:str - uri of the file
    output_file_name:str - output file name
    '''

    print(f'Downloading {uri}')
    ret = False
    try:
        r = requests.get(uri)
        r.raise_for_status()
        with open(output_file_name, mode='wb') as f:
            f.write(r.content)
            ret = True
    except Exception as e:
        print(f'Error: {e}')

    return ret

def print_highest_HourlyDryBulbTemperature(file_name:str) -> None:
    pd.set_option('display.max_columns', None)
    df = pd.read_csv(file_name)
    print('Highest HourlyDryBulbTemperature')
    print(df[df['HourlyDryBulbTemperature'] == df['HourlyDryBulbTemperature'].max()])

def main():

    # create download subdirectory if not present and make it current
    if not 'downloads' in os.listdir('.'):
        os.makedirs('downloads')
    os.chdir('downloads')

    # get html    
    html = get_html(gov_url)

    # parses html to get file path and name
    csv_relative_path, file_name = get_csv_path(html, target_date)
    file_server_url = re.search('https?://([A-Za-z_0-9.-]+)', gov_url).group(0)

    # download file and extract information to be printed
    if csv_relative_path:
        if download_file(file_server_url + csv_relative_path, file_name):
            print_highest_HourlyDryBulbTemperature(file_name)
    else:
        print('Data file not found')

if __name__ == '__main__':
    main()
