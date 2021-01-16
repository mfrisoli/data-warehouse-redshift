# Sparkify Data Warehouse

## Purpose:
Data Warehouse for Song Play Analysis.

Data is gathered from 2 separate sets, and is loaded into an optimized Star Schema that allows to run queries quickly and without the need to perform multiple joins.

Index:
1. Files and Requirements
2. Schema
3. How to Run


## 1. Files and Requirements
### 1.1 Files:
- `sql_queries.py`: Python file with all SQL queries required to create staging tables and database star schema, Copy data to staging table and inser values to each dimension and fact table.
- `create_tables.py`: Python file that connects to redhift cluster and runs "CREATE TABLE" queries.
- `etl.py`: Python file that connects to redhift cluster and runs the "COPY" data into staging area query and also runs the insert queries to the dimension and facts table.
- `dwh.cfg`: Configuration file to store parameters between other files.
- `awsUser.cfg` File with AWS KEY and SECRET KEY, file not included in the repo.
- `redshift_cluster.ipynb` Jupyter Notebook for creating AWS Redshift Cluster.
### 1.2 Python library Requirements
- python 3
- boto3
- json
- configparser
- psycopg2
- sql_queries


### 1.3 Sofware:
- Jupyter Notbooks

Notes:
- AWS Account:

### 1.4 Parameters for Redshift Cluster:
- DWH_CLUSTER_TYPE='multi-node'
- DWH_NUM_NODES='4'
- DWH_NODE_TYPE='dc2.large'

## 2. Schema "Star Schema"
### 2.1 Fact Table
- songplays - records in log data associated with song plays i.e. records with page NextSong
-- songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

### 2.2 Dimension Tables
- users - users in the app
-- user_id, first_name, last_name, gender, level
- songs - songs in music database
-- song_id, title, artist_id, year, duration
- artists - artists in music database
-- artist_id, name, location, latitude, longitude
- time - timestamps of records in songplays broken down into specific units
-- start_time, hour, day, week, month, year, weekday

## 3. How to Run:

With you AWS Account:
a. Create a Redshift Cluster with the parameters noted in 1.4.
b. Create IAM client to connect to the cluster

Once you hace created your Redshift Cluster, is necesary to get the Endpoint address and IAM client ARN, and save this in the `dwh.cfg` file.

Open a  terminal execute the following commands:
c. `$: python create_tables.py`, this will create the tables, if no errors occur, got to nex step.
d. `$: python etl.py`, this will load the data into the stagin area and then populate the star schema.

At this point you should be able to run queries on the data.

e. IMPORTANT: Make sure to delete your Redshift cluster and IAM client at the end as this incurs in cost.









set(section, option, value)
get(section, option, *, raw=False, vars=None[, fallback])

