 # Data Lake with Spark - Sparkify


This is the five project submission for the Udacity Data Engineering Nanodegree.

## Introduction


A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. You'll be working with two datasets that reside in S3. Here are the S3 links for each:

- Song data: `s3://udacity-dend/song_data`
- Log data: `s3://udacity-dend/log_data`


## Schema Design


The schema used for this project is the Star Schema. A fact table was created containing the data related to the events of the songs. In addition, the `songs`,` artists`, `time`, and` users` dimensions were created. This schema is the most recommended due to the need to perform JOINs between tables and the ease of returning data from this project.

![Schema Design](Schema_Design.png?Raw=true "Schema Design")


## ETL Pipeline


First, the stages events, and songs are loaded. After the loading of the fact table "songplays" is carried out. Then the dimension tables (song, artist, user, time) are loaded in parallel. Finally, a quality test is performed on the data.

![OK Execution](Execution.PNG?Raw=true "OK Execution")


## How to run the Python scripts

1 - Create Redshift cluster

2 - Setup 'AWS_Credentials' connection on Airflow

3 - Setup 'redshift' connection on Airflow

4 - Create tables on redshift query console (create_tables.sql)

5 - Run /opt/airflow/start.sh

6 - Turn On Dag

7 - Wait for scheduler or trigger Dag


## Project Structure


* dags: * The folder where Airflow reads files from Jobs.
* plugins: * Within this folder are the custom operators and helpers files.
* create_tables.sql: * File with create table queries
* Execution.PNG: * Successful Execution Job
* README.md: * (This file)
