import boto3
import os

MODEL_BUCKET_NAME = 'twcmodels'
STATUS_BUCKET_NAME = 'twcstatus'

def upload_file_to_bucket(file_path, bucket_name, file_name):
    s3 = boto3.client(
        's3',
        region_name='eu-west-2',
        aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY')
    )
    s3.upload_file(file_path, bucket_name, file_name)
