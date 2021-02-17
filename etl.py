import os
import glob
import psycopg2
import pandas as pd
import sql_queries


def get_files(filepath):
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))
    
    return all_files

def process_song_file(cur, conn, filepath):
    # open song file
    df = pd.read_json(filepath, lines=True)
    song_data = list(df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0])
    
    # insert song record
    song_table_insert = ("""
    INSERT INTO songs(song_id,title,artist_id,year,duration)VALUES(%s,%s,%s,%s,%s);
    """)
    song_data = list(df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0])
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_table_insert = ("""
    INSERT INTO artists(artist_id,name,location,latitude,longitude)VALUES(%s,%s,%s,%s,%s);
    """)
    artist_data = list(df[['artist_id','artist_name','artist_location','artist_latitude', 'artist_longitude']].values[0])
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, conn, filepath):
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
    
    time_table_insert = ("""
    INSERT INTO time(start_time,hour,day,week,month,year,weekday)VALUES(%s,%s,%s,%s,%s,%s,%s);
    """)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = logs_df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    user_table_insert = ("""
    INSERT INTO users(user_id,first_name,last_name,gender,level)VALUES(%s,%s,%s,%s,%s);
    """)
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)
        
        
    # insert songplay records
    sql_query = "SELECT DISTINCT s.title song, a.name artist, s.duration, s.song_id, a.artist_id FROM songs s, artists a WHERE s.artist_id = a.artist_id;"
    song_match_df = pd.read_sql_query(sql_query, conn)
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
    songplay_table_insert = ("""
    INSERT INTO songplay(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)VALUES(%s,%s,%s,%s,%s,%s,%s,%s);
    """)
    songplay_data = songplay_df.values[0].tolist()
    cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
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