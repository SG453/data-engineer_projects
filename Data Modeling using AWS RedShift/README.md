# Data Modeling on AWS Cloud using Amazon RedShift Database

## Project Goal
________________________________
The goal of this project is to help **Sparkify**(Online music streaming app) data analysts and data scientists by providing a Data Warehouse (Star schema) solution on AWS Cloud using RedShift Database and s3 bucket (to read the source data). Sparkify users use this data model to query and extract any meaningful insights.

## Introduction
_____________
**Sparkify** has provided their source data includes Log data and Song data in the form of JSON files in AWS s3 bucket.
* We read one log data file and one song data file to understand the source schema and create staging tables to load source data files.
* We use the COPY command to load source data from the s3 bucket into staging tables.
* We create Dim and Fact tables by understanding the data from staging tables.
* Finally, create data pipelines to load data from staging tables into Dim and Fact tables.

## Data
____________________
We have received song and log event files in the form of JSON format from Sparkify. They are stored in AWS S3 Bucket [link](https://s3.console.aws.amazon.com/s3/buckets/udacity-dend?region=us-west-2&tab=objects)

## Schema Design Justification
___________________________
After loading the source data (JSON) files into staging tables. We can notice that song data consists of song, artist information, and events data consists of user data and other facts.   
Based on this analysis we can create Song, Artist, Time, User Dimension tables, and SongPlay fact table includes keys from Dim tables plus additional facts like user agent, location, session_id, etc.  
This represents the **Star Schema** design by placing the SongPlay fact table surrounded by Dim tables.

## Data Assumptions
______________________________________
* If user has both free and paid levels. We pulled the most recent user level based on timestamp.
* Assuming the length column from staging_events table corresponds to duration in staging_songs table.


## Sample Queries
___________________
#### Query to see 5 most recent played songs
```
%%sql select distinct t2.title as "song title",t3.artistname as "artist name",t1.location as "user location",t4.firstname||' '||t4.lastname as User,
case when t4.level = 'paid' then 'Yes' else 'No' end as Subcribed
from songplay t1
join song t2 
on t1.songid = t2.songid
join artist t3
on t1.artistid = t3.artistid
join users t4
on t1.userid = t4.userid
join time t5
on t1.starttime = t5.starttime
order by t5.starttime desc
limit 5
```

#### Query to see top 5 songs played in November,2018
```
%%sql select title,count(*) as Cnt from (
    select t3.title,t1.userid
    from songplay t1
    join time t2
    on t1.starttime = t2.starttime
    join song t3
    on t1.songid = t3.songid
    where t2.year = 2018
    and t2.month = 11
    group by t3.title,t1.userid
) as A 
group by title
order by Cnt desc
limit 5
```

### Query to see top 5 Customers who listened to more songs in November,2018
```
%%sql 
select B.firstname||' '||B.lastname as User,count(*) as cnt from (
    select t3.title,t1.userid
    from songplay t1
    join time t2
    on t1.starttime = t2.starttime
    join song t3
    on t1.songid = t3.songid
    where t2.year = 2018
    and t2.month=11
    group by t3.title,t1.userid  
) as A
join users B
on A.userid = B.userid
group by B.firstname||' '||B.lastname
order by cnt desc
limit 5
```
