import configparser
import psycopg2
import boto3
from sql_queries import load_staging_queries, load_all_queries, dq_check_queries


def load_staging_tables(cur, conn):
	for query in load_staging_queries:
		cur.execute(query)
		conn.commit()

def data_quality_check(cur,conn):
    for query in dq_check_queries[:3]:
        cur.execute(query)
        r_cnt = cur.fetchone()[0]
        if r_cnt <= 0:
            raise Exception('Data Quality Check Failed. No records in staging table')
    

def insert_tables(cur, conn):
    for query in load_all_queries[:-1]:
        cur.execute(query)
        conn.commit()

def move_processed_files(s3,bucket,source_prefix,target_prefix,file_format):
    s3_bucket = s3.Bucket(bucket)
    for obj in s3_bucket.objects.filter(Prefix=source_prefix):
        if obj.key.endswith(file_format):
            k = obj.key
            copy_source = {'Bucket':bucket, 'Key':k}
            p_k = k.split('/')[-1]
            #print(p_k)
            s3.meta.client.copy(copy_source,bucket,target_prefix+p_k)
            s3.Object(bucket,k).delete()

def main():

    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    KEY = config.get('AWS','KEY')
    SECRET = config.get('AWS','SECRET')
    bucket = 'udacity-capstone-ecommerce'
    source_prefix = 'Sample_data/monthly'
    target_prefix = 'Processed_files/'
    file_format = '.csv'
    s3 = boto3.resource('s3',aws_access_key_id = KEY,aws_secret_access_key = SECRET)
    
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    # loading data into staging tables
    load_staging_tables(cur, conn)
    print('Staging Data Load Complete...!')
    
    # data quality check part 1 to check if records exists in staging tables.
    data_quality_check(cur,conn)
    print('Data Quality Check Passed.')
    
    # loading data into dimension tables
    insert_tables(cur, conn)
    print('Data load into Dimension tables Completed...!')

    # data quality check part 2 to compare the record counts.
    q4 = dq_check_queries[-2]
    cur.execute(q4)
    q4_cnt = cur.fetchone()[0]

    q5 = dq_check_queries[-1]
    cur.execute(q5)
    q5_cnt = cur.fetchone()[0]
    print(q4_cnt,q5_cnt)
    if q4_cnt != q5_cnt:
        raise Exception('Data Quality Check Failed. Record count is not matched.')

    # loading data into fact table.
    cur.execute(load_all_queries[-1])
    conn.commit()
    print('Data load into fact table complete')


    conn.close()
    
    move_processed_files(s3,bucket,source_prefix,target_prefix,file_format)
    print(f'Processed files moved to {target_prefix} successfully')


if __name__ == "__main__":
    main()