from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql_stmt = "",
                 insert_sql_stmt_included = "",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.sql_stmt = sql_stmt
        self.insert_sql_stmt_included = insert_sql_stmt_included

    def execute(self, context):        
        redshift = PostgresHook(self.redshift_conn_id)  
        self.log.info('Connected to database.')
        if self.insert_sql_stmt_included == 'no':
            insert_stmt = " Insert into {} ".format(self.table)        
            redshift.run(insert_stmt + self.sql_stmt)
        else:
            redshift.run(self.sql_stmt)
        self.log.info(f'Data loaded into {self.table}')
