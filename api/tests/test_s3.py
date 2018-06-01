import json
from unittest import TestCase
from uuid import uuid4
import os
from boto3 import Session

from api import api
from api.config import TestingConfig

sess = Session(
    region_name='eu-west-2',
    aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY')
)

MODEL_BUCKET_NAME = TestingConfig().TWC_BUCKET_NAME

from api.utils.aws import get_models, save_model, MODEL_ROOT_NAME, \
    OverwriteFailure, get_status, set_model, ModelNotFound, STATUS_FILE_NAME, get_current_model, NoModelsFound

test_version = 1205918250293

class TestS3Client(TestCase):
    delete = {'Objects': [
        {'Key': MODEL_ROOT_NAME + str(test_version)},
        {'Key': MODEL_ROOT_NAME + str(test_version+1)},
        {'Key': STATUS_FILE_NAME}
    ]}
    def setUp(self):
        api.app.config.from_object('api.config.TestingConfig')
        self.app = api.app.test_client()
        self.s3 = sess.resource('s3')
        bucket = self.s3.Bucket(MODEL_BUCKET_NAME)
        try:
            bucket.delete_objects(Delete=self.delete)
            bucket.delete()
        except:
            pass
        try:
            self.s3.Bucket(MODEL_BUCKET_NAME).create(CreateBucketConfiguration={'LocationConstraint': 'eu-west-2'})
        except:
            pass

    def tearDown(self):
        bucket = self.s3.Bucket(MODEL_BUCKET_NAME)
        bucket.delete_objects(Delete=self.delete)
        self.s3.Bucket(MODEL_BUCKET_NAME).delete()

    def test_get_models(self):
        with api.app.app_context():
            models = get_models()
            print(models)

    def test_save_models(self):
        with api.app.app_context():
            save_model('Hello', test_version)
            models = get_models()
            self.assertTrue(test_version in models)

    def test_overwrite_model(self):
        with api.app.app_context():
            save_model('Hello', test_version)
            with self.assertRaises(OverwriteFailure):
                save_model('Hello', test_version)

    def test_get_status(self):
        with api.app.app_context():
            get_status()

    def test_set_status(self):
        with api.app.app_context():
            with self.assertRaises(ModelNotFound):
                set_model(test_version)
            save_model('Hello', test_version)
            set_model(test_version)
            self.assertEqual(get_status()[-1]['current_version'], test_version)

    def test_get_current_model(self):
        with api.app.app_context():
            with self.assertRaises(NoModelsFound):
                get_current_model()
            save_model('Hello', test_version)
            save_model('Hello')
            # no status set yet, should get latest model
            self.assertEqual(get_current_model(), MODEL_ROOT_NAME + str(test_version+1))
            set_model(test_version)
            # status now set to previous, should get that model
            self.assertEqual(get_current_model(), MODEL_ROOT_NAME + str(test_version))

    def test_api_set_status(self):
        with api.app.app_context():
            save_model('hi', version=test_version)
            save_model('hi')
            self.assertEqual(get_current_model(), MODEL_ROOT_NAME + str(test_version + 1))
        self.app.put('/status', headers={'content-type': 'application/json'},
                     data=json.dumps({'version': test_version}))
        with api.app.app_context():
            self.assertEqual(get_current_model(), MODEL_ROOT_NAME + str(test_version))

    def test_get_models(self):
        with api.app.app_context():
            save_model('hi', version=test_version)
            save_model('hi')
        response = self.app.get('/models')
        self.assertEqual(len(json.loads(response.data.decode('utf-8'))), 2)

    def test_get_status(self):
        with api.app.app_context():
            save_model('hi', version=test_version)
            save_model('hi')
            set_model(test_version+1)
            set_model(test_version)
        response = self.app.get('/status')
        data = json.loads(response.data.decode('utf-8'))
        self.assertEqual(len(data), 2)
        self.assertEqual(data[-1]['current_version'], test_version)