# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplay CASCADE;"
user_table_drop = "DROP TABLE IF EXISTS users CASCADE;"
song_table_drop = "DROP TABLE IF EXISTS songs CASCADE;"
artist_table_drop = "DROP TABLE IF EXISTS artists CASCADE;"
time_table_drop = "DROP TABLE IF EXISTS time CASCADE;"

# CREATE TABLES

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS
songplay(start_time TIMESTAMP, 
         user_id int NOT NULL, 
         level char(10) NOT NULL, 
         song_id char(100), 
         artist_id char(100), 
         session_id char(100) NOT NULL, 
         location varchar(255), 
         user_agent varchar(255),
         PRIMARY KEY (start_time, user_id, session_id));
""")


user_table_create = ("""
CREATE TABLE IF NOT EXISTS
users(user_id varchar(100) NOT NULL,
      first_name varchar(255),
      last_name varchar(255), 
      gender varchar(10), 
      level varchar(10) NOT NULL,
      PRIMARY KEY (user_id, level));
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS
songs(song_id varchar(100) NOT NULL,
      title varchar(255),
      artist_id varchar(100) NOT NULL, 
      year smallint,
      duration float,
      PRIMARY KEY (song_id, artist_id));
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS
artists(artist_id varchar(100) NOT NULL,
        name varchar(255) NOT NULL,
        location varchar(255),
        latitude float(8),
        longitude float(8),
        PRIMARY KEY (artist_id));


""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS
time(t_id serial PRIMARY KEY,
     start_time TIMESTAMP NOT NULL,
     hour smallint,
     day smallint,
     week smallint,
     month smallint,
     year smallint,
     weekday smallint);
""")

# INSERT RECORDS

songplay_table_insert = ("""
INSERT INTO songplay(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)VALUES(%s,%s,%s,%s,%s,%s,%s,%s);
""")

user_table_insert = ("""
INSERT INTO users(user_id,first_name,last_name,gender,level)VALUES(%s,%s,%s,%s,%s)
ON CONFLICT (user_id,level)
DO
    UPDATE SET first_name = '[Duplicate]';

""")

song_table_insert = ("""
INSERT INTO songs(song_id,title,artist_id,year,duration)VALUES(%s,%s,%s,%s,%s);
""")

artist_table_insert = ("""
INSERT INTO artists(artist_id,name,location,latitude,longitude)VALUES(%s,%s,%s,%s,%s)
ON CONFLICT (artist_id)
DO
    UPDATE SET name = '[Duplicate]'|| ': '|| artists.name;
""")

time_table_insert = ("""
INSERT INTO time(start_time,hour,day,week,month,year,weekday)VALUES(%s,%s,%s,%s,%s,%s,%s);
""")

# FIND SONGS

song_select = ("""
SELECT s.title song, a.name artist, s.duration, s.song_id, a.artist_id
FROM songs s, artists a
WHERE s.artist_id = a.artist_id;
""")

# Match Song and Artists

song_artist_match = ("""
SELECT DISTINCT s.title song, a.name artist, s.duration, s.song_id, a.artist_id 
FROM songs s, artists a 
WHERE s.artist_id = a.artist_id;
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]


#,
#         FOREIGN KEY (time_id) REFERENCES time(time_id),
#         FOREIGN KEY (artist_id) REFERENCES artists(artist_id),
#         FOREIGN KEY (user_id) REFERENCES users(user_id),
#         FOREIGN KEY (song_id) REFERENCES songs(song_id)

#,
#      CONSTRAINT fk_artist FOREIGN KEY (artist_id) REFERENCES artists(artist_id)