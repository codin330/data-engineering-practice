from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pandas as pd
import zipfile
from pyspark.sql.window import Window
import os

nrows = 1000000

def load_zipped_csv(spark, path, csv_file_name):
    ''' Load csv from zip file returning the DataFrame

    spark - spark session
    path - zip file path
    csv_file_name - name of csv inside the zip
    '''

    # Due to lack of support from PySpark to load a zipped csv the workaround is to load it using pandas.
    with zipfile.ZipFile(path,'r') as z_file:
        with z_file.open(csv_file_name) as csv_file:
            pdf = pd.read_csv(csv_file, nrows=nrows)    #TODO remove nrows limit
            df = spark.createDataFrame(pdf)
            csv_file.close()
        z_file.close()
    return df

def add_file_name_column(df, source_file):
    ''' Add source_file as a column to the DataFrame

    df - DataFrame. 
    source_file - source_file name to be added
    '''

    return df.withColumn('source_file', F.lit(source_file))

def add_file_date_column(df):
    ''' Add file_date as a column to the DataFrame

    df - DataFrame
    '''

    return df.withColumn('file_date', F.to_date(F.substring(df['source_file'], 12, 10), 'yyyy-MM-dd'))

def add_brand_column(df):
    ''' Add brand as a column to the DataFrame

    It will be based on the column model. If the column model has a space ...
    aka in it, split on that space. The value found before the space will be 
    considered the brand. If there is no space to split on, fill in a value 
    called unknown for the brand.

    df - DataFrame
    '''

    return df.withColumn('brand', F.when(F.locate(' ', df['model']) > 0, \
        F.split(df['model'], ' ', 2)[0]) \
        .otherwise('unknown'))

def add_storage_ranking_column(df):
    ''' Add storage_ranking as a column to the DataFrame

    df - DataFrame
    '''

    # TODO Is there a way to rank withou creating a constant column?
    # Create storage ranking DataFrame
    windowSpec = Window.partitionBy('fixed').orderBy(F.desc('capacity_bytes'))
    sr_df = df.groupBy('model').agg({'capacity_bytes':'max'}) \
        .withColumnRenamed('max(capacity_bytes)','capacity_bytes') \
        .withColumn('fixed', F.lit('AAA')) \
        .withColumn('storage_ranking', F.dense_rank().over(windowSpec)) \
        .drop('fixed')
    
    # Join ranking back to DataFrame
    df = df.join(sr_df, ['model','capacity_bytes'],'left')

    return df

def add_primary_key_column(df):
    ''' Add primary_key as a column to the DataFrame

    df - DataFrame
    '''

    # A primary key for the dataset can be build using the serial_number 
    # in combination with model, since a serial must be unique for a model.
    # It is assumed that models are unique among brands and suppliers. 

    df = df.withColumn('primary_key', F.hash('model','serial_number'))

    return df

def wipe_dir(dir_path):
    ''' Remove all files and then the directory
    '''
    
    if dir_path in os.listdir():
        for f in os.listdir(dir_path):
            os.remove(dir_path + '/' + f)
        os.removedirs(dir_path)

def write_report(df, report_name, report_directory='reports'):
    ''' Export DataFrame as a csv file to the destination directory

    df - DataFrame to be exported
    report_name - CSV file name
    report_directory - destination directory, default 'reports'
    '''

    temp_path = 'temp_dir'
    wipe_dir(temp_path)
    
    df.coalesce(1).write.csv(temp_path, header=True)

    if report_directory not in os.listdir():
        os.mkdir(report_directory)

    for entry in os.listdir(temp_path):
        if (entry.endswith('.csv')):
            os.rename(temp_path + '/' + entry, report_directory + '/' + report_name)
            break
    
    wipe_dir(temp_path)

def main():
    spark = SparkSession.builder.appName('Exercise7') \
        .enableHiveSupport().getOrCreate()
    # your code here

    # data source paths
    driver_failures_zip_path = 'data/hard-drive-2022-01-01-failures.csv.zip'
    driver_failures_csv = driver_failures_zip_path.split('/')[-1]       # TODO Replace file name extraction with regexp
    driver_failures_csv = driver_failures_csv[:str(driver_failures_csv).index('.zip')]

    # Load data
    df = load_zipped_csv(spark, driver_failures_zip_path, driver_failures_csv)

    # Add source_file as a column to the DataFrame
    df = add_file_name_column(df, driver_failures_csv)

    # Add file_date as a column to the DataFrame
    df = add_file_date_column(df)

    # Add brand as a column to the DataFrame
    df = add_brand_column(df)

    # Add storage_ranking as a column to the DataFrame
    df = add_storage_ranking_column(df)

    # Add primary_key as a column to the DataFrame
    df = add_primary_key_column(df)

    write_report(df, 'hard-drive-2022-01-01-failures.add-features.csv')
    
if __name__ == '__main__':
    main()
