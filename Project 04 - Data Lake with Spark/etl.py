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
    """Create spark session..."""
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    This function is used to read data from the received directory
         (Bucket S3) and create artist and sound dimension tables.

    Parameters: 
        spark: Spark session
        input_data: path to the bucket of song data
        output_path: path to the output bucket where parquet file will be stored
    """
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*'
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id','year', 'duration').dropDuplicates()
    songs_table.createOrReplaceTempView('songs')
    
    # write songs table to parquet files partitioned by year and artist
    output_songs = output_data + '/songs/songs.parquet'
    songs_table.write.mode('overwrite').partitionBy('year', 'artist_id').parquet(output_songs)

    # extract columns to create artists table
    artists_table = df.select('artist_id', 'artist_name', 'artist_location',
                              'artist_latitude', 'artist_longitude') \
                      .withColumnRenamed('artist_name', 'name') \
                      .withColumnRenamed('artist_location', 'location') \
                      .withColumnRenamed('artist_latitude', 'latitude') \
                      .withColumnRenamed('artist_longitude', 'longitude').dropDuplicates()
    artists_table.createOrReplaceTempView('artists') 
    
    # write artists table to parquet files
    output_artists = output_data + '/artists/artists.parquet'
    artists_table.write.mode('overwrite').parquet(output_artists)


def process_log_data(spark, input_data, output_data):
    """
    This function is used to read the log data in the received path and populated
        the following dimension tables: user, time, and song. In addition, the 
        songplays fact table is populated by this function.
    """    
    # get filepath to log data file
    log_data = input_data + 'log_data/*'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df_song_plays = df.filter(df.page == 'NextSong') \
                      .select('userId', 'sessionId', 'userAgent', 'song',\
                              'artist', 'level', 'location', 'ts')\
                      .withColumnRenamed('userId', 'user_id') \
                      .withColumnRenamed('sessionId', 'session_id') \
                      .withColumnRenamed('userAgent', 'user_agent')

    # extract columns for users table    
    users_table = df.select('userId', 'firstName', 'lastName',
                            'gender', 'level')\
                    .withColumnRenamed('userId', 'user_id') \
                    .withColumnRenamed('firstName', 'first_name') \
                    .withColumnRenamed('lastName', 'last_name').dropDuplicates()
    users_table.createOrReplaceTempView('users')
    
    # write users table to parquet files
    output_users = output_data + '/users/users.parquet'
    users_table.write.mode('overwrite').parquet(output_users)

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: x / 1000.0)
    df_song_plays = df_song_plays.withColumn('timestamp', get_timestamp(df_song_plays.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x / 1000.0))
    df_song_plays = df_song_plays.withColumn('start_time', get_timestamp(df_song_plays.timestamp))
    
    # extract columns to create time table
    time_table = df_song_plays.select('start_time')                               \
                              .withColumn('start_time', df_song_plays.start_time) \
                              .withColumn('hour',       hour('start_time'))       \
                              .withColumn('day',        dayofmonth('start_time')) \
                              .withColumn('week',       weekofyear('start_time')) \
                              .withColumn('month',      month('start_time'))      \
                              .withColumn('year',       year('start_time'))       \
                              .withColumn('weekday',    dayofweek('start_time'))  \
                              .dropDuplicates()
    
    # write time table to parquet files partitioned by year and month
    output_time = output_data + '/time/time.parquet'
    time_table.write.mode('overwrite').partitionBy('year', 'month').parquet(output_time) 

    # read in song data to use for songplays table
    song_df = spark.read.json(input_data + 'song_data/*/*/*')

    # extract columns from joined song and log datasets to create songplays table 
    df_song_plays.createOrReplaceTempView("staging_songs")
    song_df.createOrReplaceTempView("staging_events")
    songplays_table = spark.sql("""
        SELECT DISTINCT a.artist_id,
                a.song_id,
                start_time,
                b.user_id,
                b.level,
                b.session_id,
                b.location,
                b.user_agent
        FROM staging_songs a
        INNER JOIN
          (SELECT user_id,
                  session_id,
                  user_agent,
                  song,
                  artist,
                  level,
                  location,
                  TIMESTAMP 'epoch' + (ts / 1000) * INTERVAL '1 second' AS start_time
           FROM staging_events) b 
               ON a.title = b.song
               AND a.artist_name = b.artist
    """)


    # write songplays table to parquet files partitioned by year and month
    output_songplays = output_data + '/songplays/songplays.parquet'
    songplays_table.write.mode('overwrite').partitionBy('year', 'month').parquet(output_songplays) 
    


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-bucket-cristian/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
