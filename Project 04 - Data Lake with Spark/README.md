 # Data Lake with Spark - Sparkify


This is the four project submission for the Udacity Data Engineering Nanodegree.

## Introduction


A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. You'll be working with two datasets that reside in S3. Here are the S3 links for each:

- Song data: `s3://udacity-dend/song_data`
- Log data: `s3://udacity-dend/log_data`


## Schema Design


The schema used for this project is the Star Schema. A fact table was created containing the data related to the events of the songs. In addition, the `songs`,` artists`, `time`, and` users` dimensions were created. This schema is the most recommended due to the need to perform JOINs between tables and the ease of returning data from this project.

![Schema Design](Schema_Design.png?Raw=true "Schema Design")

In view of this scheme, the distribution style was defined aiming at evaluations aimed at analyzes by user. Based on this focus, the characteristics of the created tables follow:
- `Songplays` table: has the` user_id` field as a distkey. It has the following compound sortkey `artist_id, song_id`.
- `users` table: has the` user_id` field as a distkey. It has the field `first_name` as sortkey.
- `songs` table: Distribution style was defined as` AUTO`, because I do not know the size of the table and I am familiarizing myself with how Redshit works. So I decided to let Redshift itself define the best form of Distribution style. It has the following compound sortkey `artist_id, song_id`.
- `artists` table: Distribution style was defined as` AUTO`, because I do not know the size of the table and I am familiarizing myself with how Redshit works. So I decided to let Redshift itself define the best form of Distribution style. It has the field `artist_id` as sortkey.
- Table `time`: Distribution style was defined as` ALL` and the field `start_time` was used as sortkey.
 


## ETL Pipeline


The ETL process is performed by the `etl.py` file. The script reads song_data and load_data from S3, transforms them to create five different tables, and writes them to partitioned parquet files in table directories on S3.


## How to run the Python scripts


1 - Run the `etl.py` file in the terminal. Here the data is processed and loaded into the appropriate tables.


## Project Structure


* etl.py: * File responsible for the ETL process.
* README.md: * (This file)
