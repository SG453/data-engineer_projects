import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')
access_id = os.environ['AWS_ACCESS_KEY_ID']=config['DEFAULT']['AWS_ACCESS_KEY_ID']
access_key = os.environ['AWS_SECRET_ACCESS_KEY']=config['DEFAULT']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    '''
    This function initiates a spark session
    '''
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    '''
    This function performs the following steps:
    -- loads the sparkify song data files (.json) format
    -- extract the songs schema columns and creates songs dimension data
    -- extract the artists schema columns and creates artist dimension data
    -- dimension tables output is written in parquet format and saved in S3 bucket
    '''  
    
    # reading all song data json files
    song_data = f'{input_data}/song_data/*/*/*/*.json'
    
    # loading songs json files into dataframe
    df = spark.read.json(song_data)

    print('************Loaded all song files******************')
    
    # selecting required columns based on songs dimension schema
    songs_table = df.select(['song_id','title','artist_name','duration','year'])
    
    # writing the output of songs dimension data in parquet format and partitioned by year,artistname
    songs_table.write.mode('overwrite').partitionBy('year','artist_name').parquet(output_data+'/songs_data_P')
    print('**************songs dimension output completed.**************')

    # selecting required columns based on artists dimension schema
    artists_table = df.select(['artist_id','artist_name','artist_latitude','artist_longitude','artist_location'])
    
    # writing the output of artists dimension data in parquet format
    artists_table.write.mode('overwrite').parquet(output_data+'/artist_data_P')
    print('*************artists dimension output completed.*************')


def process_log_data(spark, input_data, output_data):
    '''
    This function performs the following steps:
    -- loads the sparkify log files (.json) format
    -- extract the users schema columns and creates users dimension data
    -- converts the ts(milli second time) into timestamp and extract all time dimention data
    -- loads the sparkify song files (.json) format and joins them with log file to create songs_play fact data.
    -- dimension and fact tables output is written in parquet format and saved in S3 bucket
    '''
    # reading all log files
    log_data =f'{input_data}/log_data/*/*/*.json'

    # loading all log json files into dataframe
    df = spark.read.json(log_data)
    
    # filtering data by nextsong action
    df = df.filter(df.page=='NextSong')
    
    print('************Log data load process completed.**********************')
    
    # Assuming we load users with thier latest level
    df.createOrReplaceTempView('users_temp')
    
    # selecting required columns based on users dimension schema
    users_table = spark.sql('''
    with cte as
    ( select userid,firstname,lastname,gender,level,cast(max(ts)/1000 as timestamp) as t
        from users_temp 
        group by userid,firstname,lastname,gender,level
    ) 
    ,cte1 as
    (   select userid,firstname,lastname,gender,level,row_number() over(partition by userid order by t desc) as R1 
        from cte)
    select userid,firstname,lastname,gender,level from cte1 where R1 = 1
    ''')

   
    # writing users schema output in parquet format
    users_table.write.mode('overwrite').parquet(output_data+'/users_data_P')
    print('********************Users dimension data process completed.************************')

    # converting milli seconds timestamp into timestamp
    get_timestamp = udf(lambda x:datetime.fromtimestamp(x/1000).strftime('%Y-%m-%d %H:%M:%S'))
    df = df.withColumn('timestamp',get_timestamp(df.ts))
    
    # extracting all required time dimension columns
    time_table = df.select(['ts',hour('timestamp').alias('hour'),dayofmonth('timestamp').alias('day')\
                        ,weekofyear('timestamp').alias('week'),month('timestamp').alias('month')\
                        ,year('timestamp').alias('year'),'timestamp'])
    
    # writing the time dimension output partitioned by year, month in parquet format
    time_table.write.mode('overwrite').partitionBy('year','month').parquet(output_data+'/time_data_P')
    print('****************Time dimension data process completed.**********************')

    # reading song data again from source files (json)
    song_data = f'{input_data}/song_data/*/*/*/*.json'
    song_df = spark.read.json(song_data)
    
    
    song_df.createOrReplaceTempView('song_df_temp')
    df.createOrReplaceTempView('log_temp')

    # extrating the required columns as per songs play fact table
    songplays_table = spark.sql('''
    select t1.userid,t1.ts,t2.song_id,t2.artist_id,t1.sessionid,t1.useragent,t1.location
    from log_temp t1
    join song_df_temp t2
    on t1.song = t2.title
    and t1.length = t2.duration
    and t1.artist = t2.artist_name
    ''')

    # writing the songs play fact table output in parquet format
    songplays_table.write.mode('overwrite').parquet(output_data+'/songs_play_P')
    print('***************Songs play facts process completed.**********************')


def main():
    spark = create_spark_session()
    #input_data = 'data'
    input_data = "s3a://udacity-dend/"
    output_data = ""
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
