# Cloud Data Warehouse - Sparkify 


This is the third project submission for the Udacity Data Engineering Nanodegree.

## Introduction


A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. You'll be working with two datasets that reside in S3. Here are the S3 links for each:

- Song data: `s3://udacity-dend/song_data`
- Log data: `s3://udacity-dend/log_data`
Log data json path: `s3://udacity-dend/log_json_path.json`

This project seeks to create a Redshift database with tables developed to optimize analysis queries in the music database.

## Schema Design


The schema used for this project is the Star Schema. A fact table was created containing the data related to the events of the songs. In addition, the `songs`, `artists`, `time`, and `users` dimensions were created. This schema is the most recommended due to the need to perform JOINs between tables and the ease of returning data from this project. 

![Schema Design](Schema_Design.png?raw=true "Schema Design")

Diante desse esquema, o ditribution style foi definido visando avaliações voltadas para análises por usuário. Com base nesse foco, seguem as características das tabelas criadas:
- Tabelas `songplays`: possui o campo `user_id` como distkey. Possui a seguinte compound sortkey `artist_id,song_id`.
- Tabela `users`: possui o campo `user_id` como distkey. Possui o campo `first_name` como sortkey.
- Tabela `songs`: Distribution style foi definido como `AUTO`, porque não conheço o tamanho da tabela e estou me familiarizando com o funcionamento do Redshit. Por isso, resolvi deixar que o próprio Redshift defina a melhor forma de Distribution style. Possui a seguinte compound sortkey `artist_id,song_id`.
- Tabela `artists`: Distribution style foi definido como `AUTO`, porque não conheço o tamanho da tabela e estou me familiarizando com o funcionamento do Redshit. Por isso, resolvi deixar que o próprio Redshift defina a melhor forma de Distribution style. Possui o campo `artist_id` como sortkey.
- Tabela `time`: Distribution style foi definido como `ALL` e o campo `start_time` foi usado como sortkey.
 


## ETL Pipeline


The ETL process is performed by the `etl.py` file. Primeiramente esse script copia os dados de um bucket S3 para tabelas staging (staging_events_copy, staging_songs_copy). Posteriormente os dados das tabelas staging são carregados para as tabelas fato/dimensões realizando as transformações necessárias. 


## How to run the Python scripts


1 - Create and add redshift database and IAM role info to `dwh.cfg`.
2 - Run the `create_tables.py` file in the terminal. This file will create the database and its tables.
3 - Run the `etl.py` file in the terminal. Here the data is processed and loaded into the appropriate tables.


## Project Structure


* create_tables.py: * File responsible for creating the necessary database and tables.
* etl.py: * File responsible for the ETL process.
* README.md: * (This file)
* sql_queries.py: * File containing the queries used to create tables, delete tables and return information.

## Example Query


Top 5 Songs by duration (Level = 'paid' AND Gender = 'F')
```
SELECT C.name, B.title, SUM(B.duration)  
    FROM songplays A 
    JOIN songs B 
        ON A.song_id = B.song_id 
    JOIN artists C 
        ON A.artist_id = C.artist_id 
    JOIN users D 
        ON A.user_id = D.user_id   
WHERE LOWER(A.LEVEL) = 'paid'
AND D.gender = 'F'
GROUP BY C.name, B.title 
ORDER BY 3 DESC
LIMIT 5;
```
