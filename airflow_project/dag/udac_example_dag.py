from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

docs = """
## Udacity Airflow Data Pipeline Project
#### Purpose
This DAG is used to automate and monitor the ETL pipeline for Sparkify.
This DAG uses four different Operators
(stage redshift,load dimention,load fact,data quality)
We use these operators to load data from AWS S3 bucket into staging tables and finally into AWS redshift data warehouse.
Sparkify users can use data warehouse tables and perform their data analysis. 
"""

default_args = {
    'owner': 'udacity',
    'start_date':datetime.now(),
    #'start_date': datetime(2019, 11, 1),
    #'end_date':datetime(2018,12,1),
    'depends_on_past': False,
    'retries' : 3,
    'retry_delay':timedelta(minutes=0),
    'catchup':False,
    'email_on_failure':False
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )
dag.doc_md = docs

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table = 'staging_events',
    redshift_conn_id ='redshift',
    aws_credentials_id = 'aws_credentials',
    s3_bucket = 'udacity-dend',
    #s3_key = 'log_data'
    s3_key = "log_data/{execution_date.year}/{execution_date.month}/{ds}-events.json",
    file_format = 'json'
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table = 'staging_songs',
    redshift_conn_id = 'redshift',
    aws_credentials_id = 'aws_credentials',
    s3_bucket = 'udacity-dend',
    s3_key = 'song_data',
    file_format = 'json'
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    table='songplays',
    sql_stmt = SqlQueries.songplay_table_insert,
    insert_sql_stmt_included = 'no',
    redshift_conn_id='redshift'
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    table = 'users',
    sql_stmt = SqlQueries.user_table_insert,
    insert_sql_stmt_included = 'no',
    append_data = 'yes',
    redshift_conn_id='redshift'
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    table = 'songs',
    sql_stmt = SqlQueries.song_table_insert,
    insert_sql_stmt_included = 'no',
    append_data = 'yes',
    redshift_conn_id='redshift'
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    table='artists',
    sql_stmt = SqlQueries.artist_table_insert,
    insert_sql_stmt_included = 'no',
    append_data = 'yes',
    redshift_conn_id='redshift'
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    table='time',
    sql_stmt = SqlQueries.time_table_insert,
    insert_sql_stmt_included = 'no',
    append_data = 'yes',
    redshift_conn_id='redshift'
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id='redshift',
    dq_chq = [{'table':'users','expected_value':0}
              ,{'table':'artists','expected_value':0}
              ,{'table':'songs','expected_value':0}
              ,{'table':'time','expected_value':0}]    
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)



start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table
load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks
run_quality_checks >> end_operator

stage_events_to_redshift.doc_md = """
This task uses stage_redshift Operator. It connects to AWS S3 bucket and load the log events data into staging table hosted on AWS redshift database.
"""

stage_songs_to_redshift.doc_md = """
This task uses stage_redshift Operator. It connects to AWS S3 bucket and load the songs data into staging table hosted on AWS redshift database.
"""

load_songplays_table.doc_md = """
This task uses load_fact Operator. It extracts the facts from staging tables (staging_events, staging_songs) and load fact data into fact table.
"""

load_user_dimension_table.doc_md = """
This task uses load_dimension Operator. It extract the user dimension data from staging_events and load into users dimension table.
** append_data = 'yes' represents Truncate and Load Operation.
** append_data = 'no' represents append operation.
"""

load_artist_dimension_table.doc_md = """
This task uses load_dimension Operator. It extract the artist dimension data from staging_songs and load into artists dimension table.
** append_data = 'yes' represents Truncate and Load Operation.
** append_data = 'no' represents append operation.
"""

load_song_dimension_table.doc_md = """
This task uses load_dimension Operator. It extract the song dimension data from staging_songs and load into songs dimension table.
** append_data = 'yes' represents Truncate and Load Operation.
** append_data = 'no' represents append operation.
"""

load_time_dimension_table.doc_md = """
This task uses load_dimension Operator. It extract the time dimension data from staging_events and load into time dimension table.
** append_data = 'yes' represents Truncate and Load Operation.
** append_data = 'no' represents append operation.
"""

run_quality_checks.doc_md = """
This task uses data_quality Operator. It will help us to identify the dimension tables who doesn't have any records or contains any null values in their id columns.
"""
