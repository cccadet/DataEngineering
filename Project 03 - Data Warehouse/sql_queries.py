import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay_table_drop"
user_table_drop = "DROP TABLE IF EXISTS user_table_drop"
song_table_drop = "DROP TABLE IF EXISTS song_table_drop"
artist_table_drop = "DROP TABLE IF EXISTS artist_table_drop"
time_table_drop = "DROP TABLE IF EXISTS time_table_drop"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events(
    artist VARCHAR,
    auth VARCHAR,
    firstName VARCHAR,
    gender CHAR,
    itemInSession INTEGER,
    lastName VARCHAR,
    length FLOAT,
    level VARCHAR,
    location VARCHAR,
    method VARCHAR,
    page VARCHAR,
    registration FLOAT,
    sessionId INTEGER,
    song VARCHAR,
    status INTEGER,
    ts BIGINT,
    userAgent VARCHAR,
    userId INTEGER
)
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs(
    num_songs INTEGER,
    artist_id TEXT,
    artist_latitude FLOAT,
    artist_longitude FLOAT,
    artist_location VARCHAR,
    artist_name VARCHAR,
    song_id TEXT,
    title VARCHAR,
    duration FLOAT,
    year FLOAT
)
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id INT IDENTITY(0,1),
    artist_id TEXT NOT NULL,
    song_id TEXT   NOT NULL, 
    start_time TIMESTAMP NOT NULL, 
    user_id INT NOT NULL              distkey, 
    level VARCHAR, 
    session_id INT, 
    location VARCHAR, 
    user_agent VARCHAR
)
compound sortkey(artist_id,song_id)
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id INT NOT NULL          distkey, 
    first_name VARCHAR            sortkey, 
    last_name VARCHAR, 
    gender VARCHAR, 
    level VARCHAR
)
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id TEXT NOT NULL,
    title TEXT NOT NULL,
    artist_id TEXT NOT NULL,
    year INT,
    duration FLOAT
)
diststyle AUTO
compound sortkey(artist_id,song_id)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id TEXT    NOT NULL      sortkey, 
    name TEXT NOT NULL, 
    location TEXT, 
    latitude FLOAT, 
    longitude FLOAT
)
diststyle AUTO
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time TIMESTAMP NOT NULL sortkey, 
    hour INT, 
    day INT, 
    week INT, 
    month INT, 
    year INT, 
    weekday INT
)
diststyle ALL
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events FROM {}
credentials 'aws_iam_role={}'
FORMAT AS json {};
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
COPY staging_songs FROM {}
credentials 'aws_iam_role={}'
FORMAT AS json 'auto';
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (artist_id, song_id, start_time, user_id, level, session_id, location, user_agent)
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
  (SELECT userId    AS user_id,
          sessionId AS session_id,
          userAgent AS user_agent,
          song,
          artist,
          level,
          location,
          TIMESTAMP 'epoch' + (ts / 1000) * INTERVAL '1 second' AS start_time
   FROM staging_events
   WHERE page = 'NextSong' ) b 
       ON a.title = b.song
       AND a.artist_name = b.artist

""")

user_table_insert = ("""
INSERT INTO users
SELECT DISTINCT userId, firstName, lastName, gender, level
FROM (
      SELECT userId, firstName, lastName, gender, level
          FROM staging_events
      WHERE page = 'NextSong'
      AND userId IS NOT NULL
      )
""")

song_table_insert = ("""
INSERT INTO songs
SELECT
    DISTINCT song_id, 
             title, 
             artist_id, 
             year, 
             duration
FROM (
      SELECT song_id, title, artist_id, year, duration 
          FROM staging_songs
      WHERE song_id IS NOT NULL
      )
""")

artist_table_insert = ("""
INSERT INTO artists
SELECT DISTINCT artist_id, 
                artist_name, 
                artist_location, 
                artist_latitude, 
                artist_longitude
FROM staging_songs
""")

time_table_insert = ("""
INSERT INTO TIME
SELECT DISTINCT TIMESTAMP 'epoch' + (ts/1000) * INTERVAL '1 second'   AS start_time,
                EXTRACT( HOUR      FROM start_time)                   AS hour,
                EXTRACT( DAY       FROM start_time)                   AS day,
                EXTRACT( WEEKS     FROM start_time)                   AS week,
                EXTRACT( MONTH     FROM start_time)                   AS month,
                EXTRACT( YEAR      FROM start_time)                   AS year,
                EXTRACT( DAYOFWEEK FROM start_time)                   AS weekday
FROM staging_events
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
