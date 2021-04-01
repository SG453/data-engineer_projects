import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

LOG_DATA               = config.get('S3','LOG_DATA')
SONG_DATA              = config.get('S3','SONG_DATA')
ARN                    = config.get("IAM_ROLE","ARN")
# DROP TABLES

staging_events_table_drop = "drop table if exists staging_events"
staging_songs_table_drop = "drop table if exists staging_songs"
songplay_table_drop = "drop table if exists songplay"
user_table_drop = "drop table if exists users"
song_table_drop = "drop table if exists song"
artist_table_drop = "drop table if exists artist"
time_table_drop = "drop table if exists time"

# CREATE TABLES

staging_events_table_create= ("""
create table if not exists staging_events(
    artist varchar(max),
    auth varchar(50),
    firstname varchar(50),
    gender char(1),
    itemInSession smallint,
    lastName varchar(50),
    length float,
    level varchar(10),
    location varchar(500),
    method char(3),
    page varchar(500),
    registration varchar(30),
    sessionId int,
    song varchar(max),
    status int,
    ts bigint,
    userAgent varchar(max),
    userId int)

""")

staging_songs_table_create = ("""
create table if not exists staging_songs(
    num_songs int,
    artist_id varchar(5000),
    artist_latitude float,
    artist_longitude float,
    artist_location varchar(max),
    artist_name varchar(max),
    song_id varchar(100),
    title varchar(5000),
    duration float,
    year int)
""")

songplay_table_create = ("""
create table songplay(
id int IDENTITY(1,1) ,
songid varchar(50) not null,
userid int not null,
artistid varchar(50) not null,
starttime bigint not null sortkey distkey,
location varchar(1000),
sessionid int not null,
useragent varchar(max) not null
)
""")

user_table_create = ("""
create table users(
userid int not null  distkey,
firstname varchar(20) not null,
lastname varchar(20) not null,
gender char(1) not null,
level varchar(10) not null) 
""")

song_table_create = ("""
create table song(
songid varchar(100) not null ,
title varchar(4000) not null,
duration numeric(10,2) not null,
year int distkey,
artistid varchar(100)
)
""")

artist_table_create = ("""
create table artist(
artistid varchar(50) not null ,
artistname varchar(200) not null,
artistlat numeric(5,2) ,
artistlong numeric(5,2) ,
location varchar(1000)
)diststyle all;
""")

time_table_create = ("""
create table time(
starttime bigint not null sortkey distkey,
hour smallint not null,
day smallint not null,
week smallint not null,
month smallint not null,
year smallint not null,
isWeekday boolean not null)
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events from {}
    credentials 'aws_iam_role={}'
    format as json 'auto ignorecase'
""").format(LOG_DATA,ARN)

staging_songs_copy = ("""
    copy staging_songs from {}
    credentials 'aws_iam_role={}'
    format as json 'auto ignorecase'
""").format(SONG_DATA,ARN)

# FINAL TABLES

songplay_table_insert = ("""
insert into songplay(songid,userid,artistid,starttime,location,sessionid,useragent)
select t2.song_id
,t1.userid
,t2.artist_id
,replace(replace(replace(t1.timestamp::text,'-',''),':',''),' ','')::BIGINT as starttime
,t1.location
,t1.sessionid
,t1.useragent
from 
    (select *,date_trunc('second',TIMESTAMP WITHOUT TIME ZONE 'epoch' + (ts::bigint::float / 1000) * INTERVAL '1 second') as timestamp 
    from staging_events
    where page = 'NextSong') as t1
join staging_songs t2
on t1.song = t2.title
and t1.artist = t2.artist_name
and t1.length = t2.duration
""")

user_table_insert = ("""
insert into users
select userid,firstname,lastname,gender,level 
from (
        select userid,firstname,lastname,gender,level,row_number() over(partition by userid order by timestamp desc) as R1
        from(
            select *,date_trunc('second',TIMESTAMP WITHOUT TIME ZONE 'epoch' + (ts::bigint::float / 1000) * INTERVAL '1 second') as timestamp from staging_events 
            where userid is not null
        ) as A
) as B
where R1 = 1
""")

song_table_insert = ("""
insert into song
select song_id,title,duration,year,artist_id 
from staging_songs
""")

artist_table_insert = ("""
insert into artist
select distinct artist_id,artist_name ,artist_latitude ,artist_longitude,artist_location 
from staging_songs
""")

time_table_insert = ("""
insert into time
select 
replace(replace(replace(timestamp::text,'-',''),':',''),' ','')::BIGINT as starttime
,extract(Hour from timestamp) as Hour
,extract(Day from timestamp) as Day
,extract(Week from timestamp) as Week
,extract(Month from timestamp) as Month
,extract(Year from timestamp) as Year
,case when extract(DOW from timestamp) in (6,7) then True else False end as Weekend
from
(select *,date_trunc('second',TIMESTAMP WITHOUT TIME ZONE 'epoch' + (ts::bigint::float / 1000) * INTERVAL '1 second') as timestamp from staging_events
where page = 'NextSong') as A
""")


# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create,  user_table_create,artist_table_create,song_table_create, time_table_create,songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [ user_table_insert, song_table_insert, artist_table_insert, time_table_insert,songplay_table_insert]
