# DROP TABLES

from ast import For


songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"


# CREATE TABLES

songplay_table_create = """
                        CREATE TABLE IF NOT EXISTS songPlays (
                        songplay_id SERIAL PRIMARY KEY,
                        start_time BIGINT NOT NULL, 
                        user_id VARCHAR, 
                        level VARCHAR, 
                        song_id VARCHAR, 
                        artist_id VARCHAR, 
                        session_id VARCHAR, 
                        location VARCHAR, 
                        useragent VARCHAR ,
                        FOREIGN KEY (user_id) REFERENCES users(user_id),
                        FOREIGN KEY (song_id) REFERENCES songs(song_id),
                        FOREIGN KEY (artist_id) REFERENCES artists(artist_id),
                        FOREIGN KEY (start_time) REFERENCES time(timestamp)
                        );
                    """

user_table_create = """
                        CREATE TABLE IF NOT EXISTS users (
                        user_id VARCHAR PRIMARY KEY, 
                        firstname VARCHAR, 
                        lastname VARCHAR, 
                        gender VARCHAR, 
                        level VARCHAR
                        );
                    """

song_table_create = """
                        CREATE TABLE IF NOT EXISTS songs (
                        song_id VARCHAR PRIMARY KEY, 
                        title VARCHAR, 
                        artist_id VARCHAR NOT NULL UNIQUE, 
                        year int, 
                        duration float
                        );
                    """

artist_table_create = """
                        CREATE TABLE IF NOT EXISTS artists(
                        artist_id VARCHAR PRIMARY KEY, 
                        name VARCHAR, 
                        location VARCHAR, 
                        latitude float, 
                        longitude float,
                        CONSTRAINT fk_song
                            FOREIGN KEY (artist_id) REFERENCES songs(artist_id)
                        );
                    """

time_table_create = """
                        CREATE TABLE IF NOT EXISTS time(
                        timestamp BIGINT PRIMARY KEY, 
                        hour int, 
                        day int, 
                        week VARCHAR, 
                        month int, 
                        year int, 
                        weekday VARCHAR
                        );
                    """

# INSERT RECORDS

songplay_table_insert = """INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, useragent) VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (songplay_id) DO NOTHING;"""

user_table_insert = """INSERT INTO users (user_id, firstname, lastname, gender, level) VALUES (%s,%s,%s,%s,%s) ON CONFLICT (user_id) 
                        DO UPDATE SET level = EXCLUDED.level;"""

song_table_insert = """INSERT INTO songs (song_id, title, artist_id, year, duration) VALUES (%s,%s,%s,%s,%s) ON CONFLICT (artist_id) DO NOTHING;"""

artist_table_insert = """INSERT INTO artists (artist_id, name, location, latitude, longitude) VALUES (%s,%s,%s,%s,%s) ON CONFLICT (artist_id) DO NOTHING;"""

time_table_insert = """INSERT INTO time (timestamp, hour, day, week, month, year, weekday) VALUES (%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (timestamp) DO NOTHING;"""

# FIND SONGS

song_select = """SELECT s.song_id, s.artist_id FROM songs s 
                JOIN artists a ON s.artist_id = a.artist_id
                WHERE s.title = %s AND a.name = %s AND s.duration = %s
            """

# QUERY LISTS

create_table_queries = [
    song_table_create,
    artist_table_create,
    time_table_create,
    user_table_create,
    songplay_table_create,
]
drop_table_queries = [
    songplay_table_drop,
    user_table_drop,
    song_table_drop,
    artist_table_drop,
    time_table_drop,
]
