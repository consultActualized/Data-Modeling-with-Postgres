import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
from datetime import datetime


def getTimeDet(timestamp):
    """
    This function is used for:
    breaking down timestamp into its base components like year, month, week, day, hour, day of week and returns an array containing all thes individual elements

    Inputs: raw timestamp
    Return: array of timestamp components
    """

    stamp = datetime.fromtimestamp(timestamp / 1000)
    return [
        timestamp,
        stamp.hour,
        stamp.day,
        stamp.strftime("%V"),
        stamp.month,
        stamp.year,
        stamp.strftime("%A"),
    ]


def process_song_file(cur, filepath):
    """
    This function is used for processing the song file:
    - Read song data JSON file as a dataframe
    - pick relevant columns related to song and artist information
    - insert this data into songs table and artists table via two calls to cur.execute statement

    Inputs:
    - cursor to database to perform cur.execute
    - filepath - json filepath of song file

    Return: nothing
    """

    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    songcols = "song_id title artist_id year duration".split()
    song_data = df[songcols]
    song_data = list(song_data.values)[0]
    cur.execute(song_table_insert, song_data)

    # insert artist record
    artistcols = "id name location latitude longitude".split()
    artistcols = ["artist_" + x for x in artistcols]
    artist_data = df[artistcols]
    artist_data = list(artist_data.values)[0]
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    This function is used for processing the log file:
    - Read log data JSON file as a dataframe
    - Filter dataframe by nextsong action
    Process Time Data:
    - retrieve individual time components from getTimeDet function by passing timestamp as input
    - collate all this time information and insert into time table via cur.execute
    Process User Data:
    - pick user information from this data frame
    - insert user data into users table via cur.execute
    Process Songplays Data:
    - using the song_select query, find the song_id, artist_id for every song title, artist and duration
    - collate all songplays related information from the log data and insert into songplays table via cur.execute

    Inputs:
    - cursor -  cursor to database to perform cur.execute
    - filepath - json filepath of log file

    Return: nothing
    """
    # open log file
    df = pd.read_json(filepath, lines=True)
    dropCols = "auth itemInSession method registration status".split()
    log_info = df.drop(dropCols, axis=1)

    # filter by NextSong action
    df = log_info[log_info["page"] == "NextSong"]

    # convert timestamp column to datetime
    time_data = list(df["ts"].map(lambda x: getTimeDet(x)))

    # insert time data records
    column_labels = "ts hour day week month year weekday".split()
    time_df = pd.DataFrame(time_data, columns=column_labels)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    usercols = "userId firstName lastName gender level".split()
    user_df = log_info[usercols]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():

        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (
            row.ts,
            row.userId,
            row.level,
            songid,
            artistid,
            row.sessionId,
            row.location,
            row.userAgent,
        )
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):

    """
    This function is used to:
    - retrieve all JSON files from the soource directory
    - iterate over these files and pass them into the process_song_file and process_log_file to update the database with relevant information from the source data

    Inputs:
    - cursor - cursor to database to perform cur.execute
    - connection - database connection
    - filepath - source data filepath
    - func - function for process song or process log
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, "*.json"))
        for f in files:
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print("{} files found in {}".format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print("{}/{} files processed.".format(i, num_files))


def main():
    """
    main function:
    This is used to:"
    - Establish connect to databae
    - Create cursor for database
    - Call process song and process log functions
    - Close database connection
    """

    conn = psycopg2.connect(
        "host=127.0.0.1 dbname=sparkifydb user=student password=student"
    )
    cur = conn.cursor()

    process_data(cur, conn, filepath="data/song_data", func=process_song_file)
    process_data(cur, conn, filepath="data/log_data", func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
