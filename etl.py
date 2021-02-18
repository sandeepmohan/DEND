import os
import glob
import psycopg2
import pandas as pd
import sql_queries


def process_song_file(cur, conn, filepath):
    
    """
    Description: This function is responsible for transforming the song files and loading the relevant fields into the schema with objective to parse song and artist data and load it into the database

    Arguments:
        cur: the cursor object.
        conn: connection to the database.
        filepath: log data or song data file path.

    Returns:
        - A dataframe created by reading in the parsed json files containing song and artist data named 'df'
        - list objects for the transformed values of above in preparation to load into the relevant tables
        
    """
    
    # open song file
    df = pd.read_json(filepath, lines=True)
    song_data = list(df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0])
    
    # insert song record
    song_data = list(df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0])
    cur.execute(sql_queries.song_table_insert, song_data)
    
    # insert artist record
    artist_data = list(df[['artist_id','artist_name','artist_location','artist_latitude', 'artist_longitude']].values[0])
    cur.execute(sql_queries.artist_table_insert, artist_data)


def process_log_file(cur, conn, filepath):
    
    """
    Description: This function is responsible for transforming the listener session logs files and loading the relevant fields into the schema

    Arguments:
        cur: the cursor object.
        conn: connection to the database.
        filepath: log data or song data file path.

    Returns:
        - Three dataframes: 'logs_df', 'next_song_df', 'song_match_df' and 'combined_df' created as a result of transformation steps
        - list objects for the values of above in preparation to load into the relevant tables
    """
    
    # open log file
    logs_df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    next_song_df = logs_df[logs_df['page']=='NextSong']

    # convert timestamp column to datetime
    next_song_df['ts'] = pd.to_datetime(next_song_df['ts'], unit='ms')
    next_song_df['hour'] = pd.DatetimeIndex(next_song_df['ts']).hour
    next_song_df['day'] = pd.DatetimeIndex(next_song_df['ts']).day
    next_song_df['weekofyear'] = pd.DatetimeIndex(next_song_df['ts']).weekofyear
    next_song_df['month'] = pd.DatetimeIndex(next_song_df['ts']).month
    next_song_df['year'] = pd.DatetimeIndex(next_song_df['ts']).year
    next_song_df['weekday'] = pd.DatetimeIndex(next_song_df['ts']).weekday
    
    # insert time data records
    time_df = next_song_df[['ts', 'hour','day', 'weekofyear','month','year','weekday']]
    

    for i, row in time_df.iterrows():
        cur.execute(sql_queries.time_table_insert, list(row))

    # load user table
    user_df = logs_df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(sql_queries.user_table_insert, row)
        
        
    # insert songplay records
    song_match_df = pd.read_sql_query(sql_queries.song_artist_match, conn)
    combined_df = pd.merge(song_match_df, next_song_df,  how='right', left_on=['artist','song'], right_on = ['artist','song'])
    songplay_df = combined_df[['ts','userId','level', 'song_id', 'artist_id', 'sessionId','location','userAgent']]
    
    #for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        #cur.execute(song_select, (row.song, row.artist, row.length))
        #results = cur.fetchone()
        
        #if results:
        #    songid, artistid = results
        #else:
        #    songid, artistid = None, None

    # insert songplay record
    songplay_data = songplay_df.values[0].tolist()
    cur.execute(sql_queries.songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    
    """
    Description: This function is responsible for listing the files in a directory,
    and then executing the ingest process for each file according to the function
    that performs the transformation to save it to the database.

    Arguments:
        cur: the cursor object.
        conn: connection to the database.
        filepath: log data or song data file path.
        func: function that transforms the data and inserts it into the database.

    Returns:
        None
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
        func(cur, conn, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()