# Data Modeling with Postgres - Sparkify Postgres ETL


This is the first project submission for the Udacity Data Engineering Nanodegree. 

## Introduction


A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

They'd like a data engineer to create a Postgres database with tables designed to optimize queries on song play analysis, and bring you on the project. Your role is to create a database schema and ETL pipeline for this analysis. You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.

## Schema Design


The schema used for this exercise is the Star Schema: There is one main fact table containing all the measures associated with each event songplays, and 4-dimensional tables songs, artists, users and time, each with a primary key that is being referenced from the fact table.

On why to use a relational database for this case:

The data types are structured (we know before-hand the structure of the jsons we need to analyze, and where and how to extract and transform each field)
The amount of data we need to analyze is not big enough to require big data related solutions.
This structure will enable the analysts to aggregate the data efficiently
Ability to use SQL that is more than enough for this kind of analysis
We need to use JOINS for this scenario

The star schema has 1 fact table (songplays), and 4 dimension tables (users, songs, artists, time). DROP, CREATE, INSERT, and SELECT queries are defined in sql_queries.py. create_tables.py uses functions create_database, drop_tables, and create_tables to create the database sparkifydb and the required tables.


## ETL Pipeline

Extract, transform, load processes in etl.py populate the songs and artists tables with data derived from the JSON song files, data/song_data. Processed data derived from the JSON log files, data/log_data, is used to populate time and users tables. A SELECT query collects song and artist id from the songs and artists tables and combines this with log file derived data to populate the songplays fact table.


## How to run the Python scripts

Run create_tables.py from terminal to set up the database and tables.
Run etl.py from terminal to process and load data into the database.
Launch test.ipynb to run validation and example queries.



## Project Structure 


Files used on the project:

data folder nested at the home of the project, where all needed jsons reside.
sql_queries.py contains all your SQL queries, and is imported into the files bellow.
create_tables.py drops and creates tables. You run this file to reset your tables before each time you run your ETL scripts.
test.ipynb displays the first few rows of each table to let you check your database.
etl.ipynb reads and processes a single file from song_data and log_data and loads the data into your tables.
etl.py reads and processes files from song_data and log_data and loads them into your tables.
README.md current file, provides discussion on my project.


Discuss the purpose of this database in the context of the startup, Sparkify, and their analytical goals.
State and justify your database schema design and ETL pipeline.
[Optional] Provide example queries and results for song play analysis.