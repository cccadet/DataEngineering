import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_brazil_cities_coordinates_drop = "DROP TABLE IF EXISTS staging_brazil_cities_coordinates"
staging_brazil_population_2019_drop = "DROP TABLE IF EXISTS staging_brazil_population_2019"
staging_brazil_covid19_cities_drop = "DROP TABLE IF EXISTS staging_brazil_covid19_cities"
coronavirus_table_drop = "DROP TABLE IF EXISTS coronavirus"
cities_table_drop = "DROP TABLE IF EXISTS cities"
health_region_table_drop = "DROP TABLE IF EXISTS health_region"
region_table_drop = "DROP TABLE IF EXISTS region"
state_table_drop = "DROP TABLE IF EXISTS state"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_brazil_cities_coordinates_create = """
CREATE TABLE IF NOT EXISTS "staging_brazil_cities_coordinates" (
state_code INT,
city_code INT,
city_name VARCHAR,
lat FLOAT,
long FLOAT,
capital BOOLEAN
)
"""


staging_brazil_population_2019_create = """
CREATE TABLE IF NOT EXISTS "staging_brazil_population_2019" (
region VARCHAR,
state VARCHAR,
city VARCHAR,
state_code INT,
city_code INT,
health_region_code INT,
health_region VARCHAR,
population INT
)
"""


staging_brazil_covid19_cities_create = """
CREATE TABLE IF NOT EXISTS "staging_brazil_covid19_cities" (
date DATE,
state VARCHAR,
name VARCHAR,
code FLOAT,
cases FLOAT,
deaths FLOAT
)
"""


coronavirus_table_create = ("""
CREATE TABLE IF NOT EXISTS "coronavirus" (
  "coronavirus_id" int PRIMARY KEY IDENTITY(0,1),
  "date_id" date,
  "city_id" int,
  "health_region_id" int distkey,
  "region_id" int,
  "state_id" int,
  "cases" float,
  "deaths" float
)
compound sortkey(state_id,region_id,city_id)
""")


cities_table_create = (""" 
CREATE TABLE IF NOT EXISTS "cities" (
  "city_id" int PRIMARY KEY sortkey,
  "city" text,
  "state_id" int,
  "lat" float,
  "long" float,
  "capital" boolean,
  "population" int
)
diststyle ALL
""")


health_region_table_create = (""" 
CREATE TABLE IF NOT EXISTS "health_region" (
  "health_region_id" int PRIMARY KEY distkey,
  "health_region" varchar sortkey
)
""")


region_table_create = (""" 
CREATE TABLE IF NOT EXISTS "region" (
  "region_id" int PRIMARY KEY IDENTITY(0,1) sortkey,
  "region" varchar
)
diststyle ALL
""")


state_table_create = (""" 
CREATE TABLE IF NOT EXISTS "state" (
  "state_id" int PRIMARY KEY sortkey,
  "state" text
)
diststyle ALL
""")


time_table_create = (""" 
CREATE TABLE IF NOT EXISTS "time" (
  "date_id" date PRIMARY KEY sortkey,
  "day" int,
  "week" int,
  "month" int,
  "year" int,
  "weekday" int
)
diststyle ALL
""")

# STAGING TABLES

staging_brazil_cities_coordinates = ("""
COPY staging_brazil_cities_coordinates(state_code,city_code,city_name,lat,long,capital) FROM {}
credentials 'aws_iam_role={}'
DELIMITER ','
IGNOREHEADER 1
csv;
""").format(config['S3']['CITIES_COORDINATES'], config['IAM_ROLE']['ARN'])

staging_brazil_population_2019 = ("""
COPY staging_brazil_population_2019(region,state,city,state_code,city_code,health_region_code,health_region,population)  FROM {}
credentials 'aws_iam_role={}'
DELIMITER ','
IGNOREHEADER 1
csv;
""").format(config['S3']['POPULATION_2019'], config['IAM_ROLE']['ARN'])

staging_brazil_covid19_cities = ("""
COPY staging_brazil_covid19_cities(date,state,name,code,cases,deaths) FROM {}
credentials 'aws_iam_role={}'
DELIMITER ','
IGNOREHEADER 1
csv;
""").format(config['S3']['COVID19_CITIES'], config['IAM_ROLE']['ARN'])

# FINAL TABLES

cities_table_insert = ("""
INSERT INTO CITIES
SELECT DISTINCT CAST(LEFT(co.city_code,6) AS INT) AS city_id,
                co.city_name AS city,
                co.state_code AS state_id,
                co.lat,
                co.long,
                co.capital,
                po.population
FROM staging_brazil_cities_coordinates co
left JOIN staging_brazil_population_2019 po
ON co.city_name = po.city
AND co.state_code = po.state_code 
""")

health_region_table_insert = ("""
INSERT INTO HEALTH_REGION
SELECT DISTINCT health_region_code AS health_region_id,
                health_region
FROM staging_brazil_population_2019
""")

region_table_insert = ("""
INSERT INTO REGION (region)
SELECT DISTINCT region
FROM staging_brazil_population_2019
""")

state_table_insert = ("""
INSERT INTO STATE
SELECT DISTINCT state_code AS state_id,
                state
FROM staging_brazil_population_2019
""")

time_table_insert = ("""
INSERT INTO TIME
SELECT DISTINCT date                                            AS date_id,
                EXTRACT( DAY       FROM date)                   AS day,
                EXTRACT( WEEKS     FROM date)                   AS week,
                EXTRACT( MONTH     FROM date)                   AS month,
                EXTRACT( YEAR      FROM date)                   AS year,
                EXTRACT( DAYOFWEEK FROM date)                   AS weekday
FROM staging_brazil_covid19_cities
""")

## FACT

coronavirus_table_insert = ("""
INSERT INTO CORONAVIRUS (date_id, city_id, health_region_id, region_id, state_id, cases, deaths)
SELECT DISTINCT 
  ci.date                  AS date_id,
  ci.code                  AS city_id,
  po.health_region_code    AS health_region_id,
  dr.region_id,
  c.state_id               AS  state_id,
  COALESCE(ci.cases, 0)    AS cases,
  COALESCE(ci.deaths, 0)   AS deaths
FROM staging_brazil_covid19_cities ci
JOIN cities c
ON ci.code = c.city_id
LEFT JOIN staging_brazil_population_2019 po
ON c.city_id = po.city_code
AND c.state_id = po.state_code 
LEFT JOIN region dr
ON po.region = dr.region
""")

#DATA QUALITY

unique_state = ("""
SELECT case when count(*) = count(distinct state_id) then 'Check state count: OK' else 'Check state count: Error' end 
FROM state
""")

coronavirus_null_city_id = ("""
SELECT case when count(*) > 0 then 'Check coronavirus.city_id: Error' else 'Check coronavirus.city_id: OK' end 
FROM coronavirus where city_id is null
""")


# QUERY LISTS

#CREATE
create_table_queries = [staging_brazil_cities_coordinates_create, staging_brazil_population_2019_create, staging_brazil_covid19_cities_create, coronavirus_table_create, cities_table_create, health_region_table_create, region_table_create, state_table_create, time_table_create]
#DROP
drop_table_queries = [staging_brazil_cities_coordinates_drop, staging_brazil_population_2019_drop, staging_brazil_covid19_cities_drop, coronavirus_table_drop, cities_table_drop, health_region_table_drop, region_table_drop, state_table_drop, time_table_drop]
#COPY
copy_table_queries = [staging_brazil_cities_coordinates, staging_brazil_population_2019, staging_brazil_covid19_cities]
#INSERT
insert_table_queries = [cities_table_insert, region_table_insert, health_region_table_insert, state_table_insert, time_table_insert]
#FACT_INSERT
fact_insert = [coronavirus_table_insert]
#DATA_QUALITY
data_quality_queries = [unique_state, coronavirus_null_city_id]