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
                        timestamp BIGINT, 
                        user_id VARCHAR, 
                        level VARCHAR, 
                        song_id VARCHAR, 
                        artist_id VARCHAR, 
                        session_id VARCHAR, 
                        location VARCHAR, 
                        useragent VARCHAR 
                        );
                    """

user_table_create = """
                        CREATE TABLE IF NOT EXISTS users (
                        user_id VARCHAR, 
                        firstname VARCHAR, 
                        lastname VARCHAR, 
                        gender VARCHAR, 
                        level VARCHAR 
                        );
                    """

song_table_create = """
                        CREATE TABLE IF NOT EXISTS songs (
                        song_id VARCHAR, 
                        title VARCHAR, 
                        artist_id VARCHAR, 
                        year int, 
                        duration float 
                        );
                    """

artist_table_create = """
                        CREATE TABLE IF NOT EXISTS artists(
                        artist_id VARCHAR, 
                        name VARCHAR, 
                        location VARCHAR, 
                        latitude float, 
                        longitude float 
                        );
                    """

time_table_create = """
                        CREATE TABLE IF NOT EXISTS time(
                        timestamp BIGINT, 
                        hour int, 
                        day int, 
                        week VARCHAR, 
                        month int, 
                        year int, 
                        weekday VARCHAR 
                        );
                    """

# INSERT RECORDS

songplay_table_insert = """INSERT INTO songplays VALUES (%s,%s,%s,%s,%s,%s,%s,%s);"""

user_table_insert = """INSERT INTO users VALUES (%s,%s,%s,%s,%s);"""

song_table_insert = """INSERT INTO songs VALUES (%s,%s,%s,%s,%s);"""

artist_table_insert = """INSERT INTO artists VALUES (%s,%s,%s,%s,%s);"""

time_table_insert = """INSERT INTO time VALUES (%s,%s,%s,%s,%s,%s,%s);"""

# FIND SONGS

song_select = """SELECT s.song_id, s.artist_id FROM songs s 
                JOIN artists a ON s.artist_id = a.artist_id
                WHERE s.title = %s AND a.name = %s AND s.duration = %s
            """

# QUERY LISTS

create_table_queries = [
    songplay_table_create,
    user_table_create,
    song_table_create,
    artist_table_create,
    time_table_create,
]
drop_table_queries = [
    songplay_table_drop,
    user_table_drop,
    song_table_drop,
    artist_table_drop,
    time_table_drop,
]
