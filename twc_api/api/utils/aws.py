import datetime
from json import JSONDecodeError

import boto3
import os
import tempfile
import pickle
import json
from io import BytesIO
import logging

import shutil
from botocore.exceptions import ClientError
from flask import current_app

STATUS_FILE_NAME = 'twc_status'
MODEL_ROOT_NAME = 'twc_model_'
ACTIVE_RUN_LOGFILE_NAME = 'current_retrain_run.log'
TRAINING_BUCKET = 'twc-input'

if 'SERVERTYPE' in os.environ and os.environ['SERVERTYPE'] == 'AWS Lambda':
    sess = boto3.Session()
else:
    sess = boto3.Session()

s3 = sess.resource('s3')

def bucket_name():
    return current_app.config['TWC_BUCKET_NAME']

def upload_file_to_bucket(file_path, bucket_name, file_name):
    bucket = s3.Bucket(bucket_name)
    bucket.upload_file(file_path, file_name)

def get_models():
    bucket = s3.Bucket(bucket_name())
    return {int(o.key.split('_')[-1]): {'key': o.key,
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

class NoModelsFound(Exception):
    pass


def get_status():
    try:
        file_content = s3.Object(bucket_name(), STATUS_FILE_NAME).get()['Body'].read().decode('utf-8')
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
    if int(version) not in models:
        raise ModelNotFound()
    else:
        new_entry = {
            'current_version': int(version),
            'timestamp': str(datetime.datetime.now())
        }
        status = get_status()
        status.append(new_entry)
        file = tempfile.NamedTemporaryFile('wb', delete=False)
        with open(file.name, 'w') as fh:
            json.dump(status, fh)
        upload_file_to_bucket(file.name, bucket_name(), STATUS_FILE_NAME)
        os.remove(file.name)

def get_current_model_key():
    models = get_models()
    status = get_status()

    if len(models) == 0:
        raise NoModelsFound()

    max_model = max(models.keys())

    if len(status) == 0:
        return models[max_model]['key']

    current_version = status[-1]['current_version']
    if current_version in models:
        return models[current_version]['key']
    else:
        return models[max_model]['key']

def load_model_into_memory(model_key):
    b = BytesIO()
    bucket = s3.Bucket(bucket_name())
    bucket.download_fileobj(model_key, b)
    b.seek(0)
    model_dict = pickle.load(b)
    return model_dict

def save_model(filename, logfile, logfile_name):
    upload_file_to_bucket(filename, bucket_name(), filename)
    upload_file_to_bucket(logfile, TRAINING_BUCKET, logfile_name)

def load_train_file_into_memory(filename):
    b = BytesIO()
    bucket = s3.Bucket(TRAINING_BUCKET)
    bucket.download_fileobj(filename, b)
    b.seek(0)
    input_file = json.load(b)
    return input_file


def next_model_name():
    models = get_models()
    top_model_id = max(models.keys())
    next_model_id = top_model_id + 1
    return MODEL_ROOT_NAME + str(top_model_id + 1), next_model_id

def sync_log_to_s3(logger):
    file_handlers = [fh for fh in logger.handlers if type(fh)==logging.FileHandler]
    if file_handlers:
        fh = file_handlers[0]
        upload_file_to_bucket(fh.baseFilename, TRAINING_BUCKET, ACTIVE_RUN_LOGFILE_NAME)

def clear_log_file_from_s3():
    s3.Object(TRAINING_BUCKET, ACTIVE_RUN_LOGFILE_NAME).delete()

def download_log_from_s3():
    s3.Bucket(TRAINING_BUCKET).download_file(ACTIVE_RUN_LOGFILE_NAME, 'retrain_output.log')

def get_training_log_json():
    try:
        return_value = s3.Object(TRAINING_BUCKET, ACTIVE_RUN_LOGFILE_NAME).get()['Body'].read().decode('utf-8')
    except ClientError as ex:
        return_value = 'No model currently running'
    return return_value
    
