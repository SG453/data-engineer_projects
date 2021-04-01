from datetime import datetime, timedelta
from airflow import DAG
import logging
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,LoadDimensionOperator,S3ToS3UsingBoto3Operator)
from helpers import CapStoneSqlQueries

docs = """
## Airflor Data Pipelines for eCommerce Project
#### Purpose
This DAG is used to automate and monitor the ETL data pipelines for eCommerce Project.
This DAG uses four different Operators
(StageToRedshiftOperator, LoadFactOperator,LoadDimensionOperator,S3ToS3UsingBoto3Operator)
We use these operators to load data from AWS S3 bucket into staging tables and finally into AWS redshift data warehouse.
Later move the processed files from source to archive folder in AWS S3.
We can use this dimension data model and perform some data analysis on online shopping activity. 
"""

def data_quality_check1(*args,**kwargs):
    """
    This function is used to check if records exists in source file. 
    If the source file doesn't contain any records. It will raise an error.
    It uses PostgresHook to connect to AWS redshift database and takes table name as parameter.
    """
    table = kwargs['params']['table']
    redshift_hook = PostgresHook('redshift')
    records = redshift_hook.get_records(f'select count(1) from {table}')
    # records validation in staging tables.
    if len(records) < 1 or len(records[0]) < 1:
        raise ValueError(f'Data Quality check failed. {table} return no results')
    num_records = records[0][0]
    if num_records < 1:
        raise ValueError(f'Data Quality check failed. {table} contained 0 rows')        
    logging.info(f'Data quality on table {table} check passed')
    
def data_quality_check2():
    query1 = CapStoneSqlQueries.dq_query1
    query2 = CapStoneSqlQueries.dq_query2
    redshift_hook = PostgresHook('redshift')
    r1_cnt = redshift_hook.get_records(query1)[0][0]
    r2_cnt = redshift_hook.get_records(query2)[0][0]
    logging.info(f'r1_cnt: {r1_cnt}, r2_cnt: {r2_cnt}')
    if r1_cnt != r2_cnt:
        raise ValueError('Record count doesn"t match. Check dq_query1 and dq_query2')
    logging.info('Data quality check2 passed')
    

default_args = {
    'owner': 'Subhash Goud',
    'start_date':datetime.now(),
    #'start_date': datetime(2019, 11, 1),
    #'end_date':datetime(2018,12,1),
    'depends_on_past': False,
    'retries' : 3,
    'retry_delay':timedelta(minutes=0),
    'catchup':False,
    'email_on_failure':False
}

dag = DAG('capstone_project_dag',
          default_args=default_args,
          description='Load and transform eCommerce data in Redshift using Airflow',
          schedule_interval= '@monthly'
        )

dag.doc_md = docs

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_multi_data_to_redshift = StageToRedshiftOperator(
    task_id='Stage_shopping_activity_multi',
    dag=dag,
    table = 'staging_shopping_activity_multi',
    redshift_conn_id ='redshift',
    aws_credentials_id = 'aws_credentials',
    s3_bucket = 'udacity-capstone-ecommerce',
    s3_key = "Sample_data/monthly/multi",
    file_format = 'csv'
)
                 
stage_electronics_data_to_redshift = StageToRedshiftOperator(
    task_id='Stage_shopping_activity_electronics',
    dag=dag,
    table = 'staging_shopping_activity_electronics',
    redshift_conn_id ='redshift',
    aws_credentials_id = 'aws_credentials',
    s3_bucket = 'udacity-capstone-ecommerce',
    s3_key = "Sample_data/monthly/electronics",
    file_format = 'csv'
)
                 
stage_jewelry_data_to_redshift = StageToRedshiftOperator(
    task_id='Stage_shopping_activity_jewelry',
    dag=dag,
    table = 'staging_shopping_activity_jewelry',
    redshift_conn_id ='redshift',
    aws_credentials_id = 'aws_credentials',
    s3_bucket = 'udacity-capstone-ecommerce',
    s3_key = "Sample_data/monthly/jewelry",
    file_format = 'csv'
)

load_product_dimension_table = LoadDimensionOperator(
    task_id='Load_product_dimension_data',
    dag=dag,
    table = 'dim_product',
    sql_stmt = CapStoneSqlQueries.load_dimProduct,
    insert_sql_stmt_included = 'yes',
    append_data = 'yes',
    redshift_conn_id='redshift'
)

load_brand_dimension_table = LoadDimensionOperator(
    task_id='Load_brand_dimension_data',
    dag=dag,
    table = 'dim_brand',
    sql_stmt = CapStoneSqlQueries.load_dimBrand,
    insert_sql_stmt_included = 'yes',
    append_data = 'yes',
    redshift_conn_id='redshift'
)


load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dimension_data',
    dag=dag,
    table='dim_time',
    sql_stmt = CapStoneSqlQueries.load_dimTime,
    insert_sql_stmt_included = 'yes',
    append_data = 'yes',
    redshift_conn_id='redshift'
)

load_shopping_fact_table = LoadFactOperator(
    task_id='Load_shopping_fact_data',
    dag=dag,
    table='fact_shoppingactivity',
    sql_stmt = CapStoneSqlQueries.load_factData,
    insert_sql_stmt_included = 'yes',
    redshift_conn_id='redshift'
)
                 
check_multi_data = PythonOperator(
    task_id='check_multi_data',
    dag=dag,
    python_callable=data_quality_check1,
    provide_context=True,
    params={'table':'staging_shopping_activity_multi'}
)
                 
check_electronics_data = PythonOperator(
    task_id='check_electronics_data',
    dag=dag,
    python_callable=data_quality_check1,
    provide_context=True,
    params={'table':'staging_shopping_activity_electronics'}
)
                 
                 
check_jewelry_data = PythonOperator(
    task_id='check_jewelry_data',
    dag=dag,
    python_callable=data_quality_check1,
    provide_context=True,
    params={'table':'staging_shopping_activity_jewelry'}
)

check_record_count = PythonOperator(
    task_id='check_record_count',
    dag=dag,
    python_callable=data_quality_check2
)
    

move_files = S3ToS3UsingBoto3Operator(
    task_id = 'move_processed_files',
    dag = dag,
    aws_credentials_id = 'aws_credentials',
    s3_bucket = 'udacity-capstone-ecommerce',
    source_prefix = 'Sample_data/monthly',
    target_prefix = 'Processed_files/',
    file_format = '.csv'
   
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

                 
                 
start_operator >> stage_multi_data_to_redshift
start_operator >> stage_electronics_data_to_redshift
start_operator >> stage_jewelry_data_to_redshift                
stage_multi_data_to_redshift >> check_multi_data
stage_electronics_data_to_redshift >> check_electronics_data
stage_jewelry_data_to_redshift >> check_jewelry_data
check_multi_data >> load_product_dimension_table
check_multi_data >> load_brand_dimension_table
check_multi_data >> load_time_dimension_table
check_electronics_data >> load_product_dimension_table
check_electronics_data >> load_brand_dimension_table
check_electronics_data >> load_time_dimension_table
check_jewelry_data >> load_product_dimension_table
check_jewelry_data >> load_brand_dimension_table
check_jewelry_data >> load_time_dimension_table
load_product_dimension_table >> check_record_count
load_brand_dimension_table >> check_record_count
load_time_dimension_table >> check_record_count
check_record_count >> load_shopping_fact_table
load_shopping_fact_table >> move_files
move_files >> end_operator


stage_multi_data_to_redshift.doc_md = """
This task uses StageToRedshiftOperator Operator.
Functionality: This task will load the multi data file (online shopping activity of multiple categories) from S3 bucket into staging table of AWS redshift database.
"""

stage_electronics_data_to_redshift.doc_md = """
This task uses StageToRedshiftOperator Operator.
Functionality: This task will load the electronics data file (online shopping activity of electronic products) from S3 bucket into staging table of AWS redshift database.
"""

stage_jewelry_data_to_redshift.doc_md = """
This task uses StageToRedshiftOperator Operator.
Functionality: This task will load the jewelry data file (consists of online shopping activity of jewelry products) from S3 bucket into staging table of AWS redshift database.
"""
check_multi_data.doc_md = """
This task uses data_quality_check Python function.
Functionality: This task will check if data exists in multi data file. If no records found it will raise an error"""

check_electronics_data.doc_md = """
This task uses data_quality_check Python function.
Functionality: This task will check if data exists in electronics data file. If no records found it will raise an error"""

check_jewelry_data.doc_md = """
This task uses data_quality_check Python function.
Functionality: This task will check if data exists in jewelry data file. If no records found it will raise an error"""

load_product_dimension_table.doc_md = """
This task uses LoadDimensionOperator Operator.
Functionality: This task will load the product dimension data from above mentioned 3 staging tables into product dimension table.
"""

load_brand_dimension_table.doc_md = """
This task uses LoadDimensionOperator Operator.
Functionality: This task will load the brand dimension data from above mentioned 3 staging tables into brand dimension table.
"""

load_time_dimension_table.doc_md = """
This task uses LoadDimensionOperator Operator.
Functionality: This task will load the time dimension data from above mentioned 3 staging tables into time dimension table.
"""

load_shopping_fact_table.doc_md = """
This task uses LoadFactOperator Operator.
Functionality: This task will load the shopping activity facts data from above mentioned 3 staging tables into shopping activity fact table. 
"""

move_files.doc_md = """
This task uses S3ToS3UsingBoto3Operator Operator.
Functionality: This task will help us to move the processed source files to archive folder after loading the data into AWS Redshift database.
"""
