from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import boto3

class S3ToS3UsingBoto3Operator(BaseOperator):
    #template_fields = ("s3_key",)
    @apply_defaults
    
    def __init__(self,
                 aws_credentials_id="",
                 s3_bucket ="",
                 source_prefix="",
                 target_prefix="",
                 file_format = "",
                 *args, **kwargs):
        super(S3ToS3UsingBoto3Operator,self).__init__(*args, **kwargs)
        self.aws_credentials_id = aws_credentials_id
        self.s3_bucket = s3_bucket
        self.source_prefix = source_prefix
        self.target_prefix = target_prefix
        self.file_format = file_format
        
    def execute(self,context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        aws_access_key_id = credentials.access_key
        aws_secret_access_key = credentials.secret_key
        s3 = boto3.resource('s3',
                            aws_access_key_id = aws_access_key_id,
                            aws_secret_access_key = aws_secret_access_key
                           )
        bucket = s3.Bucket(self.s3_bucket)
        for obj in bucket.objects.filter(Prefix=self.source_prefix):
            if obj.key.endswith(self.file_format):
                k = obj.key
                copy_source = {'Bucket':self.s3_bucket, 'Key':k}
                p_k = k.split('/')[-1]
                s3.meta.client.copy(copy_source,self.s3_bucket,self.target_prefix+p_k)
                s3.Object(self.s3_bucket,k).delete()
        self.log.info(f'Processed files moved to {self.target_prefix} successfully.')
                
        
       
            
        