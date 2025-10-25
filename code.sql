--  CREATING NEW DB --

CREATE OR REPLACE DATABASE Sparkify_db;

// CREATING SCHEMAS 
CREATE OR REPLACE SCHEMA staging;
CREATE OR REPLACE SCHEMA dwh;

// CREATING FILE FORMAT STAGES AND TABLES // 

USE SCHEMA staging;

CREATE OR REPLACE FILE FORMAT json_format 
  TYPE = 'JSON';

CREATE OR REPLACE STAGE song_stage
 url='s3://affan-sparkify-data-warehousing-project/songs_data/'
  credentials=(aws_key_id='' aws_secret_key='')
  file_format = json_format;

CREATE OR REPLACE STAGE log_stage
 url='s3://affan-sparkify-data-warehousing-project/logs_data/'
  credentials=(aws_key_id=' aws_secret_key='')
  file_format = json_format;

LIST @song_stage;
LIST @log_stage;

CREATE OR REPLACE TABLE staging.song_data (record VARIANT);
CREATE OR REPLACE TABLE staging.log_data (record VARIANT);

// COPYING DATA FROM STAGE TO TABLE //

COPY INTO staging.song_data
FROM @song_stage
FILE_FORMAT = (FORMAT_NAME = json_format)
ON_ERROR = 'SKIP_FILE';

COPY INTO staging.log_data
FROM @log_stage
FILE_FORMAT = (FORMAT_NAME = json_format)
ON_ERROR = 'SKIP_FILE';

// CHECKING //

SELECT COUNT(*) FROM log_data
SELECT COUNT(*) FROM song_data

--------------------------------------------------------------------------------------------

                // Data Warehousing - Facts and Dimesnions Tables //

USE SCHEMA dwh;

 --  DIMENSIONS TABLES  -- 

-- SONGS
CREATE OR REPLACE TABLE songs (
    song_id VARCHAR PRIMARY KEY,
    title VARCHAR,
    artist_id VARCHAR,
    year INT,
    duration FLOAT
);

INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT 
    record:song_id::VARCHAR AS song_id,
    record:title::VARCHAR AS title,
    record:artist_id::VARCHAR AS artist_id,
    record:year::INT AS year,
    record:duration::FLOAT AS duration
FROM staging.song_data
WHERE record:song_id IS NOT NULL
GROUP BY song_id, title, artist_id, year, duration;


-- ARTISTS
CREATE OR REPLACE TABLE artists (
    artist_id VARCHAR PRIMARY KEY,
    name VARCHAR,
    location VARCHAR,
    latitude FLOAT,
    longitude FLOAT
);

INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT 
    record:artist_id::VARCHAR AS artist_id,
    record:artist_name::VARCHAR AS name,
    record:artist_location::VARCHAR AS location,
    record:artist_latitude::FLOAT AS latitude,
    record:artist_longitude::FLOAT AS longitude
FROM staging.song_data
WHERE record:artist_id IS NOT NULL
GROUP BY artist_id, name, location, latitude, longitude;



-- USERS
CREATE OR REPLACE TABLE users (
    user_id INT PRIMARY KEY,
    first_name VARCHAR,
    last_name VARCHAR,
    gender VARCHAR,
    level VARCHAR
);

INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT 
    record:userId::INT AS user_id,
    record:firstName::VARCHAR AS first_name,
    record:lastName::VARCHAR AS last_name,
    record:gender::VARCHAR AS gender,
    record:level::VARCHAR AS level
FROM staging.log_data
WHERE record:page::VARCHAR = 'NextSong'
AND record:userId IS NOT NULL
GROUP BY user_id, first_name, last_name, gender, level;


-- TIME
CREATE OR REPLACE TABLE time (
    start_time BIGINT PRIMARY KEY,
    hour INT,
    day INT,
    week INT,
    month INT,
    year INT,
    weekday INT
);

INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT 
    record:ts::BIGINT AS start_time,
    EXTRACT(HOUR FROM TO_TIMESTAMP(record:ts::BIGINT/1000)) AS hour,
    EXTRACT(DAY FROM TO_TIMESTAMP(record:ts::BIGINT/1000)) AS day,
    EXTRACT(WEEK FROM TO_TIMESTAMP(record:ts::BIGINT/1000)) AS week,
    EXTRACT(MONTH FROM TO_TIMESTAMP(record:ts::BIGINT/1000)) AS month,
    EXTRACT(YEAR FROM TO_TIMESTAMP(record:ts::BIGINT/1000)) AS year,
    EXTRACT(DOW FROM TO_TIMESTAMP(record:ts::BIGINT/1000)) AS weekday
FROM staging.log_data
WHERE record:page::VARCHAR = 'NextSong'
AND record:ts IS NOT NULL
GROUP BY start_time, hour, day, week, month, year, weekday;

  --  FACTS TABLES -- 

-- SONGPLAYS 
CREATE OR REPLACE TABLE songplays (
    songplay_id INT AUTOINCREMENT PRIMARY KEY,
    start_time BIGINT,
    user_id INT,
    level VARCHAR,
    song_id VARCHAR,
    artist_id VARCHAR,
    session_id INT,
    location VARCHAR,
    user_agent VARCHAR,
    FOREIGN KEY (start_time) REFERENCES time(start_time),
    FOREIGN KEY (user_id) REFERENCES users(user_id),
    FOREIGN KEY (song_id) REFERENCES songs(song_id),
    FOREIGN KEY (artist_id) REFERENCES artists(artist_id)
);

INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT 
    e.record:ts::BIGINT AS start_time,
    e.record:userId::INT AS user_id,
    e.record:level::VARCHAR AS level,
    s.record:song_id::VARCHAR AS song_id,
    s.record:artist_id::VARCHAR AS artist_id,
    e.record:sessionId::INT AS session_id,
    e.record:location::VARCHAR AS location,
    e.record:userAgent::VARCHAR AS user_agent
FROM staging.log_data e
LEFT JOIN staging.song_data s 
    ON e.record:song::VARCHAR = s.record:title::VARCHAR 
    AND e.record:artist::VARCHAR = s.record:artist_name::VARCHAR
WHERE e.record:page::VARCHAR = 'NextSong'
AND e.record:userId IS NOT NULL;


--------------------------------------------------------------------

                // TESTING TABLES FOR RESULTS //

SELECT * FROM songplays;
SELECT * FROM songs;
SELECT * FROM artists;
SELECT * FROM users;
SELECT * FROM time;
  