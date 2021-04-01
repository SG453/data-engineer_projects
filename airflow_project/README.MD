# Building ETL Data Pipelines for online music streaming app (Sparkify) using Apache Airflow

## Project Introduction
A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.
They have decided to bring you into the project and expect you to create high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills. 
They have also noted that the data quality plays a big part when analyses are executed on top the data warehouse and want to run tests against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets.

The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.

## Project Scope and Data Description
### Scope
The Scope of this project is to analyze the source data and create a data model on AWS RedShift and build ETL Data pipelines using Apache Airflow. We may have to create our own Operators as part of this project.

### Data Description
We have received song and log event files in the form of JSON format from Sparkify. They are stored in AWS S3 Bucket [link](https://s3.console.aws.amazon.com/s3/buckets/udacity-dend?region=us-west-2&tab=objects)


## Data Model
### Schema Justification
Song file consists of songs and artist information. Log event file consists of user and user activity includes song, artist, user-session, user-agent, location etc. 
Based on this information. We can create a user dimension, songs dimension, time dimension, artist dimension and songs play (fact table). With single fact table surrounded by multiple dimension tables represents **a star schema**.

### Data Granularity
We won't be aggregating any of our facts. Since we want to analyze user listening activity for smaller intervals too. 

### Steps to create a Dimension data model
We will follow the below steps to create a Dimension data model using Python and Airflow

* Construct required DDL SQL Scripts and save them.
* Construct required DML SQL Scripts and save them as a Python file.
* Create airflow DAG with required tasks and task dependencies includes (Hooks, Operators, functions)

## Building Data Pipelines
Assuming we will receive song and log files on daily basis.

### Steps to run in order to create and load data into Dimension data model
* Login into AWS and connect to RedShift database.
* Execute DDL SQL statement manually for one time to create the staging and dimension tables.link
* Open Airflow and set up connections for AWS S3 and redshift database.link
* On the DAG and Trigger it manually to load data into Dimension and Fact tables.

ETL Data pipeline using Airflow
![alt text](https://github.com/SG453/data-engineer_projects/blob/main/airflow_project/Dag_img.JPG "Airflow DAG")

## Data Quality Checks
We will perform the following data quality checks on the data and they will be the part of this ETL data pipeline.

* Check if data exists in source file. If data doesn't exists in any of the source files. Then we will force the pipeline to fail.
* Check if user, song, artist columns contain any Null values

