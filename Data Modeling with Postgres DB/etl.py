import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    This function parses the given json file via filepath parameter
    and extracts artist and song data and finally,dumps the data into artist and song tables.
    
    Input parameters:
    cur --> open cursor to execute the sql commands
    filepath--> filepath of the source file.
    """
    # open song file
    df = pd.read_json(filepath,lines=True)
    
    # insert artist record
    artist_data = df[['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']].values.tolist()[0]
    cur.execute(artist_table_insert, artist_data)

    # insert song record
    song_data = df[['song_id','title','artist_id','duration','year']].values.tolist()[0]
    cur.execute(song_table_insert, song_data)
    
    


def process_log_file(cur, filepath):
    """
    This function parses the json log file via filepath parameter.
    Filters the extracted data using a key word 'NextSong' on page column.
    Also, extract the time dimension values from timestamp column after converting the ts column from milliseconds to timestamp.
    It also extracts the users data and load into users dimension table.
    Finally, it parses the song select query to get the songid, artistid
    by passing the values from dataframe and loads the extrated values into songsplay table
    
    Input parameters:
    cur --> open cursor to execute the sql commands
    filepath--> filepath of the source file.
    """
    # open log file
    df = pd.read_json(filepath,lines=True)

    # filter by NextSong action
    df = df[df.page=='NextSong']

    # convert timestamp column to datetime
    df['ts1'] = pd.to_datetime(df['ts'],unit='ms')
    df['hour'] = df['ts1'].dt.hour
    df['day'] = df['ts1'].dt.day
    df['week'] = df['ts1'].dt.week
    df['month'] = df['ts1'].dt.month
    df['year'] = df['ts1'].dt.year
    df['weekday']=df['ts1'].dt.weekday_name
    
    # insert time data records
    time_data = df[['ts1','hour','day','week','month','year','weekday']].values.tolist()
    column_labels = (['TimeStamp','Hour','Day','Week','Month','Year','Weekday'])
    time_df = pd.DataFrame(time_data,columns=column_labels)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId','firstName','lastName','gender','level']].groupby(['userId','firstName','lastName','gender','level']).head(1)

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
#         else:
#             songid, artistid = None, None

            # insert songplay record
            songplay_data = (songid,row.ts1,row.userId,artistid,row.sessionId,row.location,row.userAgent)
            cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    This funtion takes the current database open connection and cursor along with json file path and function.
    It extracts all files with .json extension in a given filepath and feed them to the given function.
    
    Input parameters:
    cur --> open cursor to execute the sql commands
    conn --> open database connection of the database
    filepath--> filepath of the source file.
    func --> Name of the function
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
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()