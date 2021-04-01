# DROP TABLES

songplay_table_drop = "drop table if exists songplay"
user_table_drop = "drop table if exists users"
song_table_drop = "drop table if exists songs"
artist_table_drop = "drop table if exists artists"
time_table_drop = "drop table if exists time"

# CREATE TABLES
user_table_create = """create table if not exists users(
                                                        userid int primary key
                                                        ,last_name varchar
                                                        ,first_name varchar
                                                        ,gender char(1)
                                                        ,level varchar
                                                        )"""

song_table_create = """create table if not exists songs(
                                                        songid varchar primary key
                                                        ,title varchar
                                                        ,artistid varchar not null
                                                        ,duration float
                                                        ,year int
                                                        ,foreign key(artistid) 
                                                        references artists(artistid)
                                                        )"""

artist_table_create = """create table if not exists artists(
                                                            artistid varchar primary key
                                                            ,name varchar
                                                            ,location varchar
                                                            ,lat float
                                                            ,lan float
                                                            )"""

time_table_create = """create table if not exists time(
                                                        starttime timestamp primary key
                                                        ,hour int
                                                        ,day int
                                                        ,week int
                                                        ,month int
                                                        ,year int
                                                        ,weekday varchar
                                                        )"""

songplay_table_create = """create table if not exists songsplay(
                                                                songplayid serial primary key
                                                                ,songid varchar not null 
                                                                ,starttime timestamp not null
                                                                ,userid int not null
                                                                ,artistid varchar not null
                                                                ,sessionid int
                                                                ,location varchar
                                                                ,user_agent varchar
                                                                ,foreign key(songid) references songs(songid)
                                                                ,foreign key(artistid) references artists(artistid)
                                                                ,foreign key(starttime) references time(starttime)
                                                                ,foreign key(userid) references users(userid)
                                                                )"""

# INSERT RECORDS

songplay_table_insert = "Insert into songsplay(songid,starttime,userid,artistid,sessionid,location,user_agent) \
                         values (%s,%s,%s,%s,%s,%s,%s)"

user_table_insert = "Insert into users(userid,last_name,first_name,gender,level) \
                     values (%s,%s,%s,%s,%s) on conflict(userid) do update set level = Excluded.level"

song_table_insert = "Insert into songs(songid,title,artistid,duration,year) \
                        values (%s,%s,%s,%s,%s) on conflict(songid) do nothing"

artist_table_insert = "Insert into artists(artistid,name,location,lat,lan) \
                        values (%s,%s,%s,%s,%s) on conflict(artistid) do nothing"


time_table_insert = "Insert into time(starttime,hour,day,week,month,year,weekday) \
                        values(%s,%s,%s,%s,%s,%s,%s) on conflict(starttime) do nothing"

# FIND SONGS
song_select = """select t1.songid,t1.artistid 
                    from songs t1 
                    join artists t2 
                    on t1.artistid = t2.artistid 
                    where t1.title = %s and t2.name = %s and t1.duration = %s"""

# QUERY LISTS

create_table_queries = [user_table_create,artist_table_create,time_table_create,song_table_create,songplay_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]