from pyspark.sql import SparkSession
import pandas as pd
import zipfile
from pyspark.sql.types import *
from pyspark.sql.functions import col, udf, to_date, to_timestamp, substring_index, regexp_replace, second, lit
from pyspark.sql.functions import year, month, date_add
import os
import glob
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, rank, desc
from pyspark.sql.functions import isnull, when

nrows = 1000000

def add_trip_duration(df):
    ''' Return DataFrame with calculated trip_duration.

    Invalid rows are discarded.
    Assumed a trip must start and end within the same day, and 
    end_time should be greater than start_time.

    df - dataframe
    '''

    return df.where((df['trip_date'] == df['trip_date_end']) & \
                (df['end_time'] > df['start_time'])) \
            .withColumn('trip_duration', \
                df['end_time'].cast(LongType()) - df['start_time'].cast(LongType()))

def get_2019_Q4_data(spark, path):
    ''' Load csv from zip file returning the DataFrame

    Original csv fields
    trip_id,start_time,end_time,bikeid,tripduration,from_station_id,from_station_name,to_station_id,to_station_name,usertype,gender,birthyear

    Return a DataFrame with the following fields
    trip_date, trip_duration, from_station_id,from_station_name,to_station_id,to_station_name, gender, birth_year
    '''

    # Due to lack of support from PySpark to load a zipped csv the workaround is to load it using pandas.
    with zipfile.ZipFile(path,'r') as z_file:
        with z_file.open('Divvy_Trips_2019_Q4.csv') as csv_file:
            pdf = pd.read_csv(csv_file, nrows=nrows)    #TODO remove nrows limit
            schema = StructType([
                StructField("trip_id", IntegerType(), True),
                StructField("start_time", StringType(), True),
                StructField("end_time", StringType(), True),
                StructField("bikeid", IntegerType(), True),
                StructField("trip_duration", StringType(), True),
                StructField("from_station_id", IntegerType(), True),
                StructField("from_station_name", StringType(), True),
                StructField("to_station_id", IntegerType(), True),
                StructField("to_station_name", StringType(), True),
                StructField("usertype", StringType(), True),
                StructField("gender", StringType(), True),
                StructField("birth_year", FloatType(), True)
                ])
            df = spark.createDataFrame(pdf, schema)
            del pdf

            df = df.withColumn('trip_date', to_date(df.start_time)) \
                .withColumn('trip_date_end', to_date(df.end_time)) \
                .withColumn('start_time', to_timestamp(df.start_time)) \
                .withColumn('end_time', to_timestamp(df.end_time)) \
                .withColumn('birth_year', df['birth_year'].cast(IntegerType()))
            
            df = add_trip_duration(df)

            csv_file.close()
        z_file.close()
    return df['trip_date', 'trip_duration', 'from_station_id', 'from_station_name', \
              'to_station_id', 'to_station_name', 'gender', 'birth_year']

def get_2020_Q1_data(spark, path):
    ''' Load csv from zip file returning the DataFrame with only the necessario features.

    Original csv fields
    ride_id,rideable_type,started_at,ended_at,start_station_name,start_station_id,end_station_name,end_station_id,start_lat,start_lng,end_lat,end_lng,member_casual

    Return a DataFrame with the following fields
    trip_date, trip_duration, from_station_id,from_station_name,to_station_id,to_station_name, gender, birth_year
    '''

    # Due to lack of support from PySpark to load a zipped csv the workaround is to load it using pandas.
    with zipfile.ZipFile(path,'r') as z_file:
        with z_file.open('Divvy_Trips_2020_Q1.csv') as csv_file:
            pdf = pd.read_csv(csv_file, nrows=nrows)    #TODO remove nrows limit
            schema = StructType([
                StructField("ride_id", StringType(), True),
                StructField("rideable_type", StringType(), True),
                StructField("start_time", StringType(), True),
                StructField("end_time", StringType(), True),
                StructField("from_station_name", StringType(), True),
                StructField("from_station_id", IntegerType(), True),
                StructField("to_station_name", StringType(), True),
                StructField("to_station_id", FloatType(), True),   #TODO remember to reverto from Integer to FloatType
                StructField("start_lat", FloatType(), True),
                StructField("start_lng", FloatType(), True),
                StructField("end_lat", FloatType(), True),
                StructField("end_lng", FloatType(), True),
                StructField("member_casual", StringType(), True)
                ])
            df = spark.createDataFrame(pdf, schema)
            del pdf

            df = df.withColumn('trip_date', to_date(df.start_time)) \
                .withColumn('trip_date_end', to_date(df.end_time)) \
                .withColumn('start_time', to_timestamp(df.start_time)) \
                .withColumn('end_time', to_timestamp(df.end_time)) \
                .withColumn('to_station_id', df['to_station_id'].cast(IntegerType())) \
                .withColumn('gender', lit(None).cast(StringType())) \
                .withColumn('birth_year', lit(None).cast(IntegerType()))

            df = add_trip_duration(df)

            csv_file.close()
        z_file.close()
    return df['trip_date', 'trip_duration', 'from_station_id', 'from_station_name', \
              'to_station_id', 'to_station_name', 'gender', 'birth_year']

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

def report_average_trip_duration(df):
    '''Report average trip duration per day

    df - DataFrame
    '''
    
    # Calculate average trip duration
    agg_df = df.groupBy('trip_date').agg({'trip_duration':'avg'})

    # Truncate average value to second precision
    agg_df = agg_df.withColumn('avg(trip_duration)', agg_df['avg(trip_duration)'].cast(IntegerType()))

    write_report(agg_df, 'average_trip_duration.csv')

def report_trips_each_day(df):
    '''Report number of trips taken each day

    df - DataFrame
    '''
    
    agg_df = df.groupBy('trip_date').agg({'trip_date':'count'})
    write_report(agg_df, 'trips_each_day.csv')

def report_popular_station(df):
    '''Report what the most popular starting trip station for each month was

    df - DataFrame
    '''
    
    # Extract features year and month to be used in groupBy clause
    agg_df = df.withColumn('year', year('trip_date')) \
        .withColumn('month', month('trip_date'))
    
    # Aggregate number of trips per start station per month
    agg_df = agg_df.groupBy(['year','month','from_station_id']) \
        .agg({'from_station_id':'count'}) \
        .withColumnRenamed('count(from_station_id)','started_trips')
    
    # Extract top station from aggregation
    agg_df = agg_df.groupBy(['year','month']) \
        .agg({'started_trips':'max','from_station_id':'max'}) \
        .withColumnRenamed('max(started_trips)','started_trips') \
        .withColumnRenamed('max(from_station_id)','from_station_id') \
        .sort(['year','month'])
    
    write_report(agg_df, 'most_popular_starting_trip_station_each_month.csv')

def report_top_3_station(df):
    '''Report what the top 3 trip stations each day for the last two weeks were

    df - DataFrame
    '''
    
    # Get date of two weeks before latest date
    max_date_minus_two_weeks = df.agg({'trip_date':'max'}) \
            .withColumn('max(trip_date)', date_add('max(trip_date)', -14)) \
            .head()[0]

    # Filter trips form 2 weeks period and aggregate number of times
    # each station was visited, either as start or end point
    df_stations = (df.select(['trip_date', 'from_station_id']) \
        .where(df.trip_date > max_date_minus_two_weeks) \
        .withColumnRenamed('from_station_id','station_id')) \
        .union(df.select(['trip_date', 'to_station_id']) \
        .where(df.trip_date > max_date_minus_two_weeks) \
        .withColumnRenamed('to_station_id','station_id')) \
        .groupBy(['trip_date','station_id']).agg({'station_id':'count'}) \
        .withColumnRenamed('count(station_id)', 'count')

    # Filter out the 3 most visited stations per day
    windowSpec = Window.partitionBy('trip_date').orderBy(desc('count'))
    df_top3 = (df_stations.withColumn("rank",rank().over(windowSpec)))
    df_top3 = df_top3.where(df_top3.rank < 4) \
        .orderBy(['trip_date','rank','station_id'])
    
    write_report(df_top3, 'top_3_station_last_two_weeks.csv')

def report_gender_trip_average_duration(df):
    '''Report if Males or Females take longer trips on average

    df - DataFrame
    '''
    
    # Calculate gender average trip duration
    agg_df = df.select(['trip_duration','gender']) \
        .where((df.gender == 'Male') | (df.gender == 'Female')) \
        .groupBy('gender').agg({'trip_duration':'avg'})
    
    # Truncate average value to second precision
    agg_df = agg_df.withColumn('avg(trip_duration)', agg_df['avg(trip_duration)'].cast(IntegerType()))
    
    write_report(agg_df, 'gender_trip_average_duratio.csv')

def report_top_10_ages_short_long_trips(df):
    '''Report what the top 10 ages of those that take the longest trips, and shortest, is

    Assumed report request the age of the 10 longest and 10 shortest trips. 

    df - DataFrame
    '''

    # Calculate age discarding rows with invalid birth date or trip_duration
    age_df = df.where(~isnull('birth_year') & (df.birth_year > 1850) & \
                      ~isnull(df.trip_duration)) \
        .withColumn('age', year('trip_date') - df.birth_year) \
        .orderBy(['trip_duration'])

    # Calculate 10h shortest and longest times to filter rows out
    shortest_10h = age_df.select(['trip_duration']).head(10)[9]['trip_duration']
    longest_10h = age_df.select(['trip_duration']).tail(10)[0]['trip_duration']

    # Generate report for shortest trips
    top_10_df = age_df.select(['trip_duration','age']).where(age_df.trip_duration <= shortest_10h)
    write_report(top_10_df, 'top_10_ages_shortest_trips.csv')

    # Generate report for longest trips
    top_10_df = age_df.select(['trip_duration','age']).where(age_df.trip_duration >= longest_10h)
    write_report(top_10_df, 'top_10_ages_longest_trips.csv')

def main():
    spark = SparkSession.builder.appName('Exercise6') \
        .enableHiveSupport().getOrCreate()

    # your code here

    # data source paths
    path_2019_Q4 = 'data/Divvy_Trips_2019_Q4.zip'
    path_2020_Q1 = 'data/Divvy_Trips_2020_Q1.zip'

    # Load data
    df = get_2019_Q4_data(spark, path_2019_Q4).union(get_2020_Q1_data(spark, path_2020_Q1))

    # Report average trip duration per day
    report_average_trip_duration(df)

    # Report number of trips taken each day
    report_trips_each_day(df)

    # Report what the most popular starting trip station for each month was
    report_popular_station(df)

    # Report what the top 3 trip stations each day for the last two weeks were
    report_top_3_station(df)

    # Report if Males or Females take longer trips on average
    report_gender_trip_average_duration(df)

    # Report what the top 10 ages of those that take the longest trips, and shortest, is
    report_top_10_ages_short_long_trips(df)

if __name__ == '__main__':
    main()

