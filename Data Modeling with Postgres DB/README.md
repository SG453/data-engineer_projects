# Data Modeling with Postgres DB

## Project Goal
________________________________
The goal of this project is to understand the source data (JSON files) and help **Sparkify**(Online music streaming app) data analysts, data scientists by providing a data model consists of fact and dimension tables.  So that they can query against the given data model to extract any meaningful insights.

## Introduction
_____________
**Sparkify** has provided a pair of JSON files. One for song data and another one for log data.
* We would extract the data from JSON files and load them into Postgres DB by building a data model consists of fact and dimension tables. 
* We then write a couple of ETL data pipelines to transfer the data from source files into our data model on a Postgres DB. 
* Finally, we use these data from our data model to find some insights.

## Schema Design
___________________________
The Source data (JSON) files consist of customer information (user), songs information, artist information.  
So it would be ideal to create user dimension, songs dimension, time dimension, and artist dimension. we need a fact table that consists of keys from dimension tables plus additional columns from the log file (like user agent, location, session-id, etc) and we can place this fact table in the center.  
Based on the above objects we can create a simple data model with a **star schema**. 


## Test the Schema
______________________
In order to test our data model. I have executed a simple query  
`What songs users are listening too?` in test.ipyng (Jupyter Notebook)
