import pickle

from api.resources.retrain import train_model_from_json
import os
import json
import argparse
import boto3
import api

with api.app.app_context():
    INPUT_BUCKET = 'twc-input'
    MODEL_BUCKET = api.app.config.get('TWC_BUCKET_NAME')

parser = argparse.ArgumentParser(description='Retrain model')
parser.add_argument('--name', dest='name', action='store')
parser.add_argument('file', action='store')

if __name__ == '__main__':
    args = parser.parse_args()
    file_obj = args.file

    model_name = args.name
    sess = boto3.Session()
    s3 = sess.resource('s3')
    input_bucket = s3.Bucket(INPUT_BUCKET)

    try:
        input_bucket.download_file(file_obj, 'input.json')
        request = json.load(open('input.json', 'r'))
        model, message = train_model_from_json(request)
        pickle.dump(model, open(model_name, 'wb'))
        output_bucket = s3.Bucket(MODEL_BUCKET)
        output_bucket.upload_file(model_name, model_name)
        with open('{}_log.txt'.format(file_obj), 'w') as f:
            f.write(message)

    except Exception as e:
        with open('{}_log.txt'.format(file_obj), 'w') as f:
            f.write(str(e))

    input_bucket.upload_file('{}_log.txt'.format(file_obj), '{}_log.txt'.format(file_obj))