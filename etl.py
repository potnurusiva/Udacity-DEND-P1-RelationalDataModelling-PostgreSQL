import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath, conn):
    """Process the given song file in the file path, extarct the required columns from it and finally load it into the tables """
    # open song file
    df = pd.read_json(filepath, lines=True)
    df = df.replace({pd.np.nan: None})

    # insert song record
    song_data = df.iloc[:,[7,8,0,9,5]].values.tolist()
    #print(song_data)
    try:
        cur.execute(song_table_insert, song_data[0])
    except psycopg2.IntegrityError as e:
        conn.rollback()
        #print('insert failure:',e)
        #continue
    else:
        conn.commit()
    
    
    # insert artist record
    artist_data = df.iloc[[0],[0,4,2,1,3]].values.tolist()
    try:
        cur.execute(artist_table_insert, artist_data[0])
    except psycopg2.IntegrityError as e:
        conn.rollback()
        #print('insert failure:',e)
        #continue
    else:
        conn.commit()


def process_log_file(cur, filepath, conn):
    """Process the given log file in the file path, extarct the required columns from it and finally load it into the tables """
    # open log file
    df = pd.read_json(filepath, lines=True)
    df = df.replace({pd.np.nan: None})

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'],unit='ms')
    
    # insert time data records
    time_data = {'start_time': t.dt.time,'hour':t.dt.hour,'day':t.dt.day,'week':t.dt.week,'month':t.dt.month,
             'year':t.dt.year,'weekday':t.dt.weekday}
    column_labels = ['start_time','hour','day','week','month','year','weekday']
    time_df = pd.DataFrame(time_data ,columns=column_labels)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df.iloc[:,[17,2,5,3,7]]

    # insert user records
    for i, row in user_df.iterrows():
        try:
            cur.execute(user_table_insert, row)
        except psycopg2.IntegrityError as e:
            conn.rollback()
            #print('insert failure:',e)
            continue
        else:
            conn.commit()

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
        startts  = pd.to_datetime(row.ts, unit='ms') #start time in time stamp foramt
        songplay_data = [startts,row.userId,row.level,songid,artistid,row.sessionId,row.location,row.userAgent]
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """Process the data in the given file path or folder by reading each and every file and load it into the tables"""
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
        func(cur, datafile, conn)
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