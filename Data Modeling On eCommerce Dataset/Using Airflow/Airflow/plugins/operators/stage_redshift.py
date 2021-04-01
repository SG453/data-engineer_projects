from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    template_fields = ('s3_key',)
    copy_json ="""
    COPY {} 
    FROM '{}'
    ACCESS_KEY_ID '{}'
    SECRET_ACCESS_KEY '{}'
    FORMAT AS JSON 'auto ignorecase'   
    """
    
    copy_csv ="""
    COPY {} 
    FROM '{}'
    ACCESS_KEY_ID '{}'
    SECRET_ACCESS_KEY '{}'
    CSV
    IGNOREHEADER 1
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id ="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="", 
                 file_format = "",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.s3_key = s3_key
        self.s3_bucket = s3_bucket
        self.file_format = file_format
      

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        redshift.run('truncate table {}'.format(self.table))
        self.log.info('truncated data from {}'.format(self.table))
        self.log.info('Copying data from S3 to Redshift')
        rendered_key = self.s3_key.format(**context)
        s3_path = 's3://{}/{}'.format(self.s3_bucket, rendered_key)

        if self.file_format == 'json':
            formatted_str = StageToRedshiftOperator.copy_json.format(
                self.table,
                s3_path,
                credentials.access_key,
                credentials.secret_key
            )
            redshift.run(formatted_str)
        elif self.file_format == 'csv':
            formatted_str = StageToRedshiftOperator.copy_csv.format(
                self.table,
                s3_path,
                credentials.access_key,
                credentials.secret_key
            )
            redshift.run(formatted_str)
        else:
            raise ValueError (f"Invalid file fomat provided {self.file_format}")





