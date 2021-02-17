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

def process_song_file(cur, filepath):
    # open song file
    song_files = get_files('data/song_data/')
    songs = []
    for s in song_files:
        data = pd.read_json(s, lines=True)
        songs.append(data)
    song_df = pd.concat(songs, ignore_index=True )
    
    # insert song record
    song_table_insert = ("""
    INSERT INTO songs(song_id,title,artist_id,year,duration)VALUES(%s,%s,%s,%s,%s);
    """)
    song_data = song_df[['song_id','title','artist_id','year','duration']].values.tolist()
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_table_insert = ("""
    INSERT INTO artists(artist_id,name,location,latitude,longitude)VALUES(%s,%s,%s,%s,%s);
    """)
    artist_data = song_df[['artist_id','artist_name','artist_location','artist_latitude', 'artist_longitude']].values.tolist()
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    # open log file
    #logs_files = get_files(filepath)
    logs = []
    for l in logs_files:
        data = pd.read_json(l, lines=True)
        logs.append(data)
    logs_df = pd.concat(logs, ignore_index=True)

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
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = logs_df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
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
    songplay_data = songplay_df.values.tolist()
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
        func(cur, datafile)
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