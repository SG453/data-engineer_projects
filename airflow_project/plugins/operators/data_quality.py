from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 dq_chq = [],      
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.dq_chq = dq_chq
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        redshift = PostgresHook(self.redshift_conn_id)  
        for table_dict in self.dq_chq:
            table_name = table_dict['table']
            expected_value = table_dict['expected_value']
            self.log.info(f'Performing Data Quality check on {table_name}')
            records = redshift.get_records(f'select count(*) from {table_name}')
            if len(records) < 1 or len(records[0])<1:
                raise ValueError (f"Data Quality check failed. {table_name} returned no results")
            num_records = records[0][0]
            
            if num_records == expected_value:
                raise ValueError(f"Data Quality check failed. {table_name} contained 0 rows")
            
            if table_name != 'time':
                id_column = table_name[:-1]+'id'
                self.log.info(f"ID column in {table_name} is {id_column}")
                null_check = redshift.get_records(f"select count(*) from {table_name} where {id_column} is null")[0][0]
                self.log.info(null_check)
                if null_check:
                    raise ValueError(f"Data Quality check failed {table_name} null identified.")
            self.log.info(f"Data Quality on table {table_name} check passed with {records[0][0]} records")