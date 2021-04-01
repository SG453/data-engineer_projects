# Data Modeling using Spark

## Project Goal
________________________________
The goal of this project is to help **Sparkify** (Online music streaming app) data analysts and data scientists by providing a Data Warehouse (Star schema) solution on AWS S3. Dimension and Fact tables are provided in the form of Parquet files on AWS S3 bucket. So that, Sparkify users use these output files and extract any meaningful insights with the help of AWS **Schema on read feature.**

## Introduction
_____________
**Sparkify** has provided their source data includes Log data and Song data in the form of JSON files in the AWS s3 bucket.
* We read one log data file and one song data file to understand the source schema and come up with star schema includes dimension and fact tables.
* We then extract the required columns for our dimension and fact tables.
* Finally, save the extracted data to AWS S3 bucket in the form of PARQUET files.

## Data Description
___________________
We have received song and log event files in the form of JSON format from Sparkify. They are stored in AWS S3 Bucket.[link](https://s3.console.aws.amazon.com/s3/buckets/udacity-dend?region=us-west-2&tab=objects)

## Schema Design Justification
___________________________
We can read the source schema for the log file and song data file after loading a single log and song file.
We can notice that song data consists of song, artist information, and logs data consists of user data and other facts.   
Based on this analysis we can create Song, Artist, Time, User Dimension tables, and SongPlay fact table with additional facts like user agent, location, session_id, etc.  
This represents the **Star Schema** design by placing the SongPlay fact table surrounded by Dim tables.


## Data Assumptions
______________________________________
* If user has both free and paid levels. We pulled the most recent user level based on timestamp.
* Assuming the length column from staging_events table corresponds to duration in staging_songs table.




