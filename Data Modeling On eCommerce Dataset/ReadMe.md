# Data modeling on eCommerce Dataset
## Project Summary
The goal of this project is to demonstrate the **Data Engineering skills** that I learned as part of my Data Engineer Nano Degree through **Udacity**.  
In this project, I will create a Data model for an eCommerce Dataset.
I will show 2 different approaches to create a data model and build the ETL data pipelines to move the data from source to target data model.
1. Use Python to create our data model and build ETL data pipelines.
2. Use Apache Airflow to create ETL data pipelines.

In both approaches, I will use AWS S3 bucket to store our source files and AWS RedShift cluster to host our data model.


## Project Scope and Data Description

### Scope
The Scope of this project is to create a data model for an eCommerce Dataset by implementing Data Engineer skills includes  
- Analyze source data
- Schema Justification 
- Creating a data model
- Host data model on RedShift cluster
- Build data pipelines (Python or Airflow)

A complete Data Analysis to find any meaningful insights is out of scope for this project.

### Data Description
The eCommerce Dataset I used for this project is provided by the Kaggle [source link](https://www.kaggle.com/mkechinov/ecommerce-behavior-data-from-multi-category-store).  
This dataset include the shopping behavior of online customers.  
For this project, I'm using 3 different eCommerce datasets.
* Multi dataset (Contains customer shopping behavior for multi-category products)
* Electronics dataset (Contains customer purchase data for electronic products)
* Jewelry dataset (Contains customer purchase data for jewelry products)

## Data Exploration and Clean Up
### Data Exploration
Top 5 records from Multi dataset
![alt text](https://github.com/SG453/data-engineer_projects/blob/main/Data%20Modeling%20On%20eCommerce%20Dataset/images/multi_top5.JPG "Multi Dataset")

Top 5 records from Electornic dataset
![alt text](https://github.com/SG453/data-engineer_projects/blob/main/Data%20Modeling%20On%20eCommerce%20Dataset/images/electronics_top5.JPG "Electronic Dataset")

Top 5 records from Jewelry dataset
![alt text](https://github.com/SG453/data-engineer_projects/blob/main/Data%20Modeling%20On%20eCommerce%20Dataset/images/jewelry_top5.JPG "Jewelry Dataset")

### Data cleanup
* **event_time:** It has a string value "UTC" in it. We can replace the 'UTC' value and convert the column to timestamp.
* **category_code:** Multiple values are concatenated with '.'. We can split the value into multiple values and assign them to subcategories. NaN category_code values will be mapped to the 'Unknown' value in the product dimension table.
* **price:** We will delete the rows with null price values.
* **user_id:** We will delete the rows with null user_id values.
* **brand:** NaN brand values will be mapped to 'UnKnown' value in the brand dimention table.

## Data Model
### Schema Justification
Based on the screenshots of sample data. If we consider the columns like event_time, product_id, category_id, category_code, brand, price, user_id, and user_session.  
We can create a **product dimension, time dimension, brand dimension, and shopping_activity as fact tables.**   
With 1 fact table and a couple of dimension tables, We can create a **dimension data model** with a *star schema*.

### Data Granularity
If we want to analyze the customer's online shopping behavior activity for smaller Intervals? then we need to store the all facts without aggregating them.  
So, that we can query the data for smaller intervals.

### Steps to create a Dimension data model
We will follow the below steps to create a Dimension data model using just Python
1. Create a config file and store all AWS S3 bucket, key, secret, and Redshift cluster details (host, dbname, user, pwd, port)
2. Construct required DDL and DML SQL scripts and save them as a Python file.
3. Create a Python script to call the above-defined DDL SQL scripts and create tables in the AWS RedShift database.
4. Create a Python script to call the above-defined DML SQL scripts and load the data into the AWS RedShift database.

Here is the [link](https://github.com/SG453/data-engineer_projects/tree/main/Data%20Modeling%20On%20eCommerce%20Dataset/Using%20Python) for source files.

We will follow the below steps to create a Dimension data model using Python and Airflow  
1. Construct required DDL SQL Scripts and save them.
2. Construct required DML SQL Scripts and save them as a Python file.
3. Create airflow DAG with required tasks and task dependencies includes (Hooks, Operators, functions)

Here is the [link](https://github.com/SG453/data-engineer_projects/tree/main/Data%20Modeling%20On%20eCommerce%20Dataset/Using%20Airflow) for source files.

## Building Data Pipelines
Assuming we will have one file per month for each dataset.
### Steps to run in order to create and load data into Dimension data model
#### Using just Python scripts  
1. Connect to cmd
2. Navigate to working folder (To the folder where we saved the scripts)
3. install any required packages using pip install package names (like configparser, boto3, psycopg2)
4. run python create_tables.py (To create staging and dimension tables on AWS Redshift Cluster)
5. run python etl.py (To load data into dimension and fact tables.)

ETL Data pipeline execution using Python
![alt text](https://github.com/SG453/data-engineer_projects/blob/main/Data%20Modeling%20On%20eCommerce%20Dataset/images/cmd_execution.JPG "cmd Execution")

#### Using Airflow
1. Login into AWS and connect to RedShift database.
2. Execute DDL SQL statement manually for one time to create the staging and dimension tables.[link](https://github.com/SG453/data-engineer_projects/tree/main/Data%20Modeling%20On%20eCommerce%20Dataset/Using%20Airflow/Airflow)
3. Execute Dummy insert record SQL statements into product dimension and brand dimension to avoid is null queries on our data model.
4. Open Airflow and set up connections for AWS S3 and redshift database.[link](https://github.com/SG453/data-engineer_projects/tree/main/Data%20Modeling%20On%20eCommerce%20Dataset/Using%20Airflow)
5. On the DAG and Trigger it manually to load data into Dimension and Fact tables.

ETL Data pipelines using Airflow
![alt text](https://github.com/SG453/data-engineer_projects/blob/main/Data%20Modeling%20On%20eCommerce%20Dataset/images/airflow_dag_graph.JPG "Airflow DAG")

## Data Quality Checks
We will perform the following data quality checks on the data and they will be the part of my ETL data pipeline. 
1. Check if data exists in source file. If data doesn't exists in any of the source files. Then we will force the pipeline to fail.
2. Verify the record counts. The number of records we loaded in staging tables and the final load into fact table should match. (Note: Not counting any data duplicates in source file)

## Data dictionary
Here is the [link](https://github.com/SG453/data-engineer_projects/blob/main/Data%20Modeling%20On%20eCommerce%20Dataset/Data_dictionary.xlsx) for Data dictionary. To understand table structures and there fields. 

## Complete Project Write Up
* I have shown the way to create a dimension data model using 2 different approaches. 
* We can just use the Python approach and schedule the etl.py script to automate the process.
* If we want to have a visual representation of our ETL data pipelines and monitor the process. We can use Airflow.
* We need to have an SLA defined on how often we will receive the files. Based on that we can automate our process for daily/weekly/monthly.

#### Data scalability
--> If the size of our source files increased by 100%. We can scale the process by increasing the nodes in our RedShift cluster.  
--> I would consider using Apache Spark to process large files. Using Spark I can read the data directly from the S3 bucket with the help of **schema on read** and partition the data and save the data back into the S3 bucket in the form of parquet files. Later create a new Airflow dag or python script to load parquet files into our data model.

#### Data performance
--> We can create indexes on dimension and fact tables to improve the performance.  
--> We can also create materialized views for the most executed queries to avoid pressure on our data model.

## Sample Queries and Results  

Top 5 Brands  
![alt text](https://github.com/SG453/data-engineer_projects/blob/main/Data%20Modeling%20On%20eCommerce%20Dataset/images/Top5_brands.JPG "Top 5 Brands")

Product viewed the most but never Bought  
![alt text](https://github.com/SG453/data-engineer_projects/blob/main/Data%20Modeling%20On%20eCommerce%20Dataset/images/Viewed_But_Never_bought.JPG "Viewed but never bought")

Top 5 most viewed products for Oct, 2019  
![alt text](https://github.com/SG453/data-engineer_projects/blob/main/Data%20Modeling%20On%20eCommerce%20Dataset/images/Top5_products_viewed.JPG "Most viewed for Oct,2019")

