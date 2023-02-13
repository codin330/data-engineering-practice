import os
import json
import glob
import csv
import flatten_json

def convert_file(json_file_path:str) -> None:
    ''' Convert json file to csv format.

    csv file will be generated in the same directory of the json file.

    json_file_path:str - path of json file
    '''
    
    print(f'Converting file {json_file_path=}')
    with open(json_file_path, 'r') as f:
        flattened_j = flatten_json.flatten(json.load(f))
        csv_file_path = json_file_path.replace('.json','.csv')
        with open(csv_file_path, 'w') as c:
            w = csv.writer(c)
            w.writerow(flattened_j.keys())
            w.writerow(flattened_j.values())
            c.close()
    return

def main():

    path = './data'
    # Change working directory to 'data'
    if 'data' not in os.listdir():
        path = './Exercises/Exercise-4/data'

    for entry in glob.glob("**/*.json", root_dir=path, recursive=True):
        convert_file(path + '/' + entry)

if __name__ == '__main__':
    main()
