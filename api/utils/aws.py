import datetime
from json import JSONDecodeError

import boto3
import os
import tempfile
import pickle
import json

import shutil
from botocore.exceptions import ClientError

MODEL_BUCKET_NAME = 'twcmodels'
STATUS_FILE_NAME = 'model_status'
MODEL_ROOT_NAME = 'twc_model_'

sess = boto3.Session(
    region_name='eu-west-2',
    aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY')
)
s3 = sess.resource('s3')

def upload_file_to_bucket(file_path, bucket_name, file_name):
    bucket = s3.Bucket(bucket_name)
    bucket.upload_file(file_path, file_name)

def get_status():
    bucket = s3.Bucket(MODEL_BUCKET_NAME)
    bucket.download_file()

def get_models():
    bucket = s3.Bucket(MODEL_BUCKET_NAME)
    return {int(o.key.split('_')[-1]): {'name': o.key,
             'last_modified': str(o.last_modified)} for o in bucket.objects.all() if valid_name(o.key)}

def valid_name(name):
    try:
        return name.startswith(MODEL_ROOT_NAME) and int(name[len(MODEL_ROOT_NAME):]) > -1
    except:
        return False

class OverwriteFailure(Exception):
    pass

class ModelNotFound(Exception):
    pass

def save_model(model, version=None):
    models = get_models()
    if version is None:
        max_version = max([m['version'] for m in models] + [0])
        next_version = max_version + 1
    else:
        if version in models:
            raise OverwriteFailure
        next_version = version

    tf = tempfile.NamedTemporaryFile(delete=False)
    with open(tf.name, 'wb') as fh:
        pickle.dump({'model': model, 'version': next_version}, fh)
    upload_file_to_bucket(tf.name, MODEL_BUCKET_NAME, MODEL_ROOT_NAME + str(next_version))
    os.remove(tf.name)

def get_status():
    try:
        file_content = s3.Object(MODEL_BUCKET_NAME, STATUS_FILE_NAME).get()['Body'].read().decode('utf-8')
        json_content = json.loads(file_content)
        return json_content
    except ClientError as ex:
        if ex.response['Error']['Code'] == 'NoSuchKey':
            return []
        else:
            raise ex
    except JSONDecodeError:
        return []

def set_model(version):
    models = get_models()
    if version not in models:
        raise ModelNotFound()
    else:
        new_entry = {
            'current_version': version,
            'timestamp': str(datetime.datetime.now())
        }
        status = get_status()
        status.append(new_entry)
        file = tempfile.NamedTemporaryFile('wb', delete=False)
        with open(file.name, 'w') as fh:
            json.dump(status, fh)
        upload_file_to_bucket(file.name, MODEL_BUCKET_NAME, STATUS_FILE_NAME)
        os.remove(file.name)
