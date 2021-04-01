from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql_stmt = "",
                 insert_sql_stmt_included = "",
                 append_data = "",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.sql_stmt = sql_stmt
        self.insert_sql_stmt_included = insert_sql_stmt_included
        self.append_data = append_data
       

    def execute(self, context):          
        redshift = PostgresHook(self.redshift_conn_id)
        if self.append_data == 'yes':
            self.log.info(f' Deleting data from {self.table}')
            redshift.run('truncate table {}'.format(self.table))
            if self.insert_sql_stmt_included == 'no':
                insert_stmt = " Insert into {} ".format(self.table)        
                redshift.run(insert_stmt + self.sql_stmt)
            else:
                redshift.run(self.sql_stmt)
        else:
            if self.insert_sql_stmt_included == 'no':
                insert_stmt = " Insert into {} ".format(self.table)        
                redshift.run(insert_stmt + self.sql_stmt)
            else:
                redshift.run(self.sql_stmt)
        self.log.info(f'Dimension data loaded into {self.table}')
