import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    This function Create Spark session.
    Parameters:
           None
    Returns:
          None    
    """       
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    This function will be open and load the song data in JSON files on S3  to spark dataframe.
    extract useful columns to create dimension table,and then write table to parquet file that store in S3.
    Parameters:
           spark: spark session connection
           input_data: path to song data json files on S3 
           output_data: path for save parquet files on S3
    Returns:
          None    
    """     
    # get filepath to song data file
    song_data = os.path.join(input_data, "song_data/A/*/*/*.json") #
    
    # read song data file
    df = spark.read.json(song_data)
    df.createOrReplaceTempView("songView")

    # extract columns to create songs table
    songs_table = spark.sql('''SELECT DISTINCT song_id, 
                            title, 
                            artist_id, 
                            year, 
                            duration 
                            FROM songView''')
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year','artist_id').parquet(output_data+"song_t.parquet")

    # extract columns to create artists table
    artists_table = spark.sql('''SELECT DISTINCT artist_id,
                            artist_name AS name,
                            artist_latitude AS latitude,
                            artist_longitude AS longtitude,
                            artist_location AS location
                            FROM songView
                           
                            ''')
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data+"artist_t.parquet")


def process_log_data(spark, input_data, output_data):
    """
    This function will be open and load the log data in JSON files on S3  to spark dataframe.
    extract useful columns to create dimension or fact table,and then write table to parquet file that store in S3.
    Parameters:
           spark: spark session connection
           input_data: path to log data json files on S3 
           output_data: path for log parquet files on S3
    Returns:
          None    
    """      
    # get filepath to log data file
    log_data = os.path.join(input_data, "log_data/*.json")

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page=='NextSong')
    df.createOrReplaceTempView("logView")

    # extract columns for users table    
    #artists_table = Modify by kun
    users_table = spark.sql('''SELECT DISTINCT userId AS user_id, 
                            firstName AS first_name, 
                            lastName AS last_name, 
                            gender, 
                            level 
                            FROM logView''')
    
    # write users table to parquet files
    #artists_table
    users_table.write.parquet(output_data+"user_t.parquet")
    # create timestamp column from original timestamp column
    #get_timestamp = udf()
    spark.udf.register("get_hour", lambda x: datetime.fromtimestamp(x/1000.0).hour)
    spark.udf.register("get_day", lambda x: int(datetime.fromtimestamp(x/1000.0).day))
    spark.udf.register("get_year", lambda x: int(datetime.fromtimestamp(x/1000.0).year))
    spark.udf.register("get_month", lambda x: int(datetime.fromtimestamp(x/1000.0).month))
    spark.udf.register("get_ts", lambda x: datetime.fromtimestamp(x/1000.0).timestamp())
    spark.udf.register("get_weekday", lambda x: int(datetime.fromtimestamp(x/1000.0).weekday()))
    spark.udf.register("get_number_week", lambda x: int(datetime.fromtimestamp(x/1000.0).isocalendar()[1]))    
    
    # create datetime column from original timestamp column
    #get_datetime = udf()
    #df = 
    
    # extract columns to create time table
    time_table = spark.sql('''
          SELECT DISTINCT
          get_ts(ts) timestamp,
          get_hour(ts) AS hour,
          get_day(ts) AS day,
          get_month(ts) AS month,
          get_year(ts) AS year,
          get_weekday(ts) AS weekday,
          get_number_week(ts) AS week
          FROM logView 
        
          '''
          )
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year','month').parquet(output_data+"time_t.parquet")

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data+"song_t.parquet")
    song_df.createOrReplaceTempView("song")


    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = songplays_table = spark.sql('''
                            SELECT  DISTINCT 
                            logView.ts AS start_time,
                            logView.userId AS user_id, 
                            logView.level AS level, 
                            song.song_id AS song_id,
                            song.artist_id AS  artist_id, 
                            logView.sessionId AS session_id, 
                            logView.location AS  location,
                            logView.userAgent AS  user_agent 
                            FROM song 
                            JOIN logView ON (
                            song.title = logView.song    
                            AND song.duration = logView.length)
                            WHERE logView.page = 'NextSong'
                            ''' )

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year','month').parquet(output_data+"songplays_t.parquet")

def main():
    spark = create_spark_session()
    input_data = "./data/"
    output_data = "./test2/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
