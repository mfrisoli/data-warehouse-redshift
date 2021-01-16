import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

ARN = config.get('IAM_ROLE', 'ARN')

LOG_DATA = config.get('S3', 'LOG_DATA')
LOG_JSONPATH = config.get('S3', 'LOG_JSONPATH')
SONG_DATA = config.get('S3', 'SONG_DATA')

REGION = 'us-west-2'


# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events (
        artist VARCHAR,
        auth VARCHAR,
        firstName VARCHAR,
        gender VARCHAR(4),
        itemInSession INTEGER,
        lastName VARCHAR,
        length DOUBLE PRECISION,
        level VARCHAR(10),
        location VARCHAR,
        method VARCHAR(10),
        page VARCHAR,
        registration NUMERIC,
        sessionid INTEGER,
        song VARCHAR,
        status SMALLINT,
        ts TIMESTAMP,
        userAgent VARCHAR,
        user_id SMALLINT
    )
    
;""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
        num_songs SMALLINT,
        artist_id text,
        artist_latitude VARCHAR,
        artist_location VARCHAR,
        artist_longitude VARCHAR,
        artist_name VARCHAR,
        duration DOUBLE PRECISION,
        song_id VARCHAR,
        title VARCHAR,
        year SMALLINT
    )
;""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id BIGINT IDENTITY(0,1),
        start_time TIMESTAMP NOT NULL,
        user_id SMALLINT NOT NULL,
        level VARCHAR(10) NOT NULL,
        song_id VARCHAR NOT NULL,
        artist_id VARCHAR NOT NULL,
        session_id INTEGER NOT NULL,
        location VARCHAR NOT NULL,
        user_agent VARCHAR NOT NULL
    )
;""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id SMALLINT NOT NULL,
        first_name VARCHAR NOT NULL,
        last_name VARCHAR NOT NULL,
        gender VARCHAR(4) NOT NULL,
        level VARCHAR(10) NOT NULL
    )
;""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id VARCHAR NOT NULL,
        title VARCHAR NOT NULL,
        artist_id VARCHAR NOT NULL,
        year SMALLINT NOT NULL,
        duration DOUBLE PRECISION NOT NULL
    )
;""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id VARCHAR NOT NULL,
        name VARCHAR NOT NULL,
        location VARCHAR,
        latitude VARCHAR,
        longitude VARCHAR
    )
;""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time TIMESTAMP NOT NULL,
        hour SMALLINT,
        day SMALLINT,
        week SMALLINT,
        month SMALLINT,
        year SMALLINT,
        weekday SMALLINT
    )
;""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events from '{}'
    CREDENTIALS 'aws_iam_role={}'
    json '{}'
    REGION '{}'
    TIMEFORMAT as 'epochmillisecs'
""").format(LOG_DATA, ARN, LOG_JSONPATH, REGION)

staging_songs_copy = ("""
    COPY staging_songs from '{}'
    CREDENTIALS 'aws_iam_role={}'
    REGION '{}'
    json 'auto'
""").format(SONG_DATA, ARN, REGION)

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (
        start_time,
        user_id,
        level,
        song_id,
        artist_id,
        session_id,
        location, 
        user_agent)
    SELECT DISTINCT se.ts AS start_time,
           se.user_id,
           se.level,
           ss.song_id,
           ss.artist_id,
           se.sessionid,
           se.location,
           se.userAgent        
    FROM staging_events se
    JOIN staging_songs ss ON se.song=ss.title
    WHERE se.page = 'NextSong'
   ;
           
""")

user_table_insert = ("""
    INSERT INTO users (user_id,
        first_name,
        last_name,
        gender,
        level)
    SELECT DISTINCT user_id,
           firstName,
           lastName,
           gender,
           level
    FROM staging_events
    WHERE user_id IS NOT NULL
    AND user_id NOT IN (SELECT DISTINCT user_id FROM users)
   ;
""")

song_table_insert = ("""
    INSERT INTO songs (
        song_id,
        title,
        artist_id,
        year,
        duration
        )
    SELECT DISTINCT song_id,
           title,
           artist_id,
           year,
           duration
    FROM staging_songs
    WHERE song_id IS NOT NULL
    AND song_id NOT IN (SELECT DISTINCT song_id FROM songs)
    ;
""")

artist_table_insert = ("""
    INSERT INTO artists (
        artist_id,
        name,
        location,
        latitude,
        longitude)
    SELECT DISTINCT artist_id,
           artist_name,
           artist_location,
           artist_latitude,
           artist_longitude
    FROM staging_songs
    WHERE artist_id IS NOT NULL
    AND artist_id NOT IN (SELECT DISTINCT artist_id FROM artists)
        ;
""")

time_table_insert = ("""
    INSERT INTO time (
        start_time,
        hour,
        day,
        week,
        month,
        year,
        weekday)
    SELECT DISTINCT ts,
           EXTRACT(HOUR FROM ts) AS hour,
           EXTRACT(DAY FROM ts) AS day,
           EXTRACT(WEEK FROM ts) AS week,
           EXTRACT(MONTH FROM ts) AS month,
           EXTRACT(YEAR FROM ts) AS year,
           EXTRACT(DOW FROM ts) AS weekday
    FROM staging_events
    WHERE ts IS NOT NULL
    AND ts NOT IN (SELECT DISTINCT start_time FROM time)
    ;
    
    
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert, songplay_table_insert]
