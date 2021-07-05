import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    This procedure processes a song file whose filepath has been provided as an arugment.
    It extracts the song information in order to store it into the songs table.
    Then it extracts the artist information in order to store it into the artists table.

    INPUTS: 
    * cur the cursor variable
    * filepath the file path to the song file
    """
    
    df = pd.read_json(filepath, lines=True)

    # insert song record
    #Create song DF from all the song data
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0]
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    #-Create artist DF from all the song data
    artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude','artist_longitude']].values[0]
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):

    """
    This procedure processes a log file whose filepath has been provided as an arugment.
    Create DataFrame of all log files filtered by NextSong.
    It extracts the time information in order to store it into the time table.
    Then it extracts the user information in order to store it into the users table.
    Extract and inserts data for songplays table from different tables by implementing a select query.

    INPUTS: 
    * cur the cursor variable
    * filepath the file path to the log file
    """ 
    # open log file
    df = pd.read_json(filepath, lines=True)
   
    # filter by NextSong action
        # filter by NextSong action
    df = df[df['page'].str.contains("NextSong")]
    # convert timestamp column to datetime
    t = pd.to_datetime(df.ts, unit='ms') 
    df['ts'] = t
    # Sort by start_time
    df = df.sort_values('ts')
    
    # insert time data records
    time_data = list(zip(t, t.dt.hour, t.dt.day, t.dt.week, t.dt.month, t.dt.year, t.dt.weekday))
    column_labels = ("start_time", "hour", "day", "week", "month", "year", "weekday")
    time_df = pd.DataFrame(time_data, columns = column_labels, dtype = int) 
    time_df = time_df.sort_values('start_time')

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for idex, row in enumerate(df.itertuples(index=False), start=1):
   # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        songplay_data = (idex, row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    get list of all json files in the directory
    Iterate and append each json file into the same pandas DataFrame
    
    INPUTS:
    *cur: the cursor variable
    *conn: connection created to the database
    *filepath: path to data file
    *func: process function
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))
    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))
    
    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """
    ETL Pipeline for Sparkify song play data:
    
    -Instantiate a session to the Postgres database. 
    -Acquire a cursor object to process SQL queries.
    -Process both the song and the log data.    
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()