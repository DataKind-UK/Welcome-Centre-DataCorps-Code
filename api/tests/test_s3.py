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

from api.utils.aws import upload_file_to_bucket, MODEL_BUCKET_NAME, get_models, save_model, MODEL_ROOT_NAME, \
    OverwriteFailure, get_status, set_model, ModelNotFound

test_version = 1205918250293

class TestS3Client(TestCase):
    delete = {'Objects': [{'Key': MODEL_ROOT_NAME + str(test_version)}]}
    def setUp(self):
        self.s3 = sess.resource('s3')
        self.s3.Bucket(MODEL_BUCKET_NAME).delete_objects(Delete=self.delete)

    def tearDown(self):
        self.s3.Bucket(MODEL_BUCKET_NAME).delete_objects(Delete=self.delete)

    def test_get_models(self):
        models = get_models()
        print(models)

    def test_save_models(self):
        save_model('Hello', test_version)
        models = get_models()
        self.assertTrue(test_version in models)

    def test_overwrite_model(self):
        save_model('Hello', test_version)
        with self.assertRaises(OverwriteFailure):
            save_model('Hello', test_version)

    def test_get_status(self):
        get_status()

    def test_set_status(self):
        with self.assertRaises(ModelNotFound):
            set_model(test_version)
        save_model('Hello', test_version)
        set_model(test_version)
        self.assertEqual(get_status()[-1]['current_version'], test_version)


    # def test_set_model(self):
    #     save_model('Hello', test_version)
    #     with self.assertRaises(ModelNotFound):
    #         set_model(test_version+1)