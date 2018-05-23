import json
from unittest import TestCase
from uuid import uuid4
import os
from boto3 import Session

sess = Session(
    region_name='eu-west-2',
    aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY')
)

from api.utils.aws import upload_file_to_bucket, MODEL_BUCKET_NAME


class TestS3Client(TestCase):
    def setUp(self):
        self.s3 = sess.resource('s3')

    def test_s3_upload(self):
        tmpname = 'hello'
        with open(tmpname, 'w') as f:
            f.write('hello')

        upload_file_to_bucket(tmpname, MODEL_BUCKET_NAME, tmpname)
        self.s3.Bucket(MODEL_BUCKET_NAME).download_file(tmpname, tmpname)