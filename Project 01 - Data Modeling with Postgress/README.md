# Data Modeling with Postgres - Sparkify Postgres ETL


This is the first project submission for the Udacity Data Engineering Nanodegree.

## Introduction


A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

This project seeks to create a Postgres database with tables developed to optimize analysis queries in the music database.

## Schema Design


The schema used for this project is the Star Schema. A fact table was created containing the data related to the events of the songs. In addition, the `songs`, `artists`, `time`, and `users` dimensions were created. This schema is the most recommended due to the need to perform JOINs between tables and the ease of returning data from this project.

## ETL Pipeline

The ETL process is performed by the `etl.py` file. The `songs` and `artists` tables are loaded with data from JSON files from the `data/song_data` directory. The `time` and `users` tables are loaded from the JSON files of the `data/log_data` directory. The `songplays` table is the fact table and has the event data of the songs. A loop was created to insert each record to be created. In this loop, the data in the `song`, `artist`, and `length` fields are replaced by their respective ID before inserting the record in the` songplays` table.


## How to run the Python scripts

1 - Run the `create_tables.py` file in the terminal. This file will create the database and its tables.
2 - Run the `etl.py` file in the terminal. Here the data is processed and loaded into the appropriate tables.
3 - Start the file `test.ipynb` and perform the validation.



## Project Structure


* create_tables.py: * File responsible for creating the necessary database and tables.
* etl.ipynb: * Jupyter Notebook in which the ETL process was developed.
* etl.py: * File responsible for the ETL process.
* README.md: * (This file)
* sql_queries.py: * File containing the queries used to create tables, delete tables and return information.
* test.ipynb: * File used to validate the uploaded data.

## Example Query


Top 5 Songs by duration
```
SELECT C.name, B.title, SUM(B.duration)  
    FROM songplays A 
    JOIN songs B 
        ON A.song_id = B.song_id 
    JOIN artists C 
        ON A.artist_id = C.artist_id 
GROUP BY C.name, B.title 
ORDER BY 3 
LIMIT 5;
```
