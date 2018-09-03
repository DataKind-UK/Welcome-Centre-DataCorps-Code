import os
import shutil

from flask import Flask
from flask import request
from flask_restplus import Api, Resource, abort

from api.model.train import train_model_from_json
from api.utils.aws import (get_models, set_model, get_status, load_train_file_into_memory,
                           get_current_model_key, load_model_into_memory, next_model_name, save_model,
                           sync_log_to_s3, clear_log_file_from_s3, get_training_log_json)
from api.model.models import TWCModel
from api.model.transformers import ParseJSONToTablesTransformer
import json
import logging
import pickle
from zappa.async import task, get_async_response


app = Flask(__name__)
app.config.from_object('api.config.ProdConfig')
api = Api(app, title='The Welcome Centre - DataKind API', default='Endpoints', default_label='')

logging.getLogger('s3transfer').setLevel(logging.CRITICAL)

class ModelContainer(object):
    current_model = None
    current_version = None

def update_model():
    current_model_key = get_current_model_key()
    if ModelContainer.current_version != current_model_key:
        app.logger.info('Model state changed, loading model')
        model = load_model_into_memory(current_model_key)
        # model = TWCModel()
        # model.load_from_object(model_dict['model'])
        ModelContainer.current_model, ModelContainer.current_version = model, current_model_key

@api.route('/set-model')
@api.doc(description='Sets the live model version in the API',
         params={'version':'Version number of the model.'})
class SetModel(Resource):
    def get(self):
        version = request.args.get('version')
        set_model(version)
        update_model()
        return get_status()

@api.route('/models')
class Models(Resource):
    def get(self):
        models = get_models()
        return models

@api.route('/status')
class Status(Resource):
    def get(self):
        status = get_status()
        return status

@api.route('/current-model')
class Status(Resource):
    def get(self):
        return get_current_model_key()

@api.route('/score')
class Score(Resource):
    def __init__(self, api, *args, **kwargs):
        self.parser = ParseJSONToTablesTransformer()

    def post(self):
        json_data = request.get_json(force=True)
        if type(json_data) == str:
            json_data = json.loads(json_data)
        tables = self.parser.transform(json_data)
        update_model()
        return ModelContainer.current_model.predict(tables)

def run_remote_retrain(source, target):
    import boto3
    init_script = """#!/bin/bash
    echo "sudo halt" | at now + 30 minutes
    sudo yum update -y
    sudo yum install -y docker
    sudo service docker start
    sudo usermod -a -G docker ec2-user
    aws configure set region eu-west-1
    sudo $(aws ecr get-login --no-include-email)
    sudo docker pull 213288821174.dkr.ecr.eu-west-1.amazonaws.com/twc:latest
    sudo nohup sh -c 'sudo docker run 213288821174.dkr.ecr.eu-west-1.amazonaws.com/twc:latest python retrain.py --name {} {} && sudo shutdown -H now' > output 2>&1 &
    """
    try:
        EC2 = boto3.client('ec2', region_name='eu-west-1')
        instance = EC2.run_instances(
            InstanceType='t2.micro',
            ImageId='ami-ca0135b3',
            MinCount=1,  # required by boto, even though it's kinda obvious.
            MaxCount=1,
            InstanceInitiatedShutdownBehavior='terminate',  # make shutdown in script terminate ec2
            IamInstanceProfile={'Name': 'twc_retrain'},
            KeyName='twc_retrain',
            UserData=init_script.format(target, source)  # file to run on instance init.
        )

        instance_id = instance['Instances'][0]['InstanceId']

        return {'Created instance_id': instance_id}
    except:
        return {'Failed to run remote script'}

def get_logger():
    if os.path.exists('retrain_output.log'):
        os.remove('retrain_output.log')

    logger = logging.getLogger('twc_logger')
    logger.handlers = []
    logger.setLevel(logging.DEBUG)
    fh = logging.FileHandler('retrain_output.log')
    fh.setLevel(logging.DEBUG)
    sh = logging.StreamHandler()
    sh.setLevel(logging.DEBUG)
    standard_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(standard_formatter)
    sh.setFormatter(standard_formatter)
    logger.addHandler(fh)
    logger.addHandler(sh)
    return logger

@task
def run_retrain(source_filename, target=None, hyperparams=None, test=False):
    with app.app_context():  # This is used since the run_retrain requires app context 
        clear_log_file_from_s3()
        if target is None:
            target, version_number = next_model_name()

        logger = get_logger()
        logger.info('Starting retrain from source json dump {} to model file {}'.format(source_filename, target))
        sync_log_to_s3(logger)
        try:
            source = load_train_file_into_memory(source_filename)
        except Exception as ex:
            if ex.response['Error']['Code'] == '404':
                logger.error(ex)
                sync_log_to_s3(logger)
                abort(message='Retrain file not found in twc-input s3 bucket')
        try:
            _, _, _, model = train_model_from_json(source, hyperparams=hyperparams, test=test)
            pickle.dump(model, open(target, 'wb'))

            log_filename = '{}_retrain_{}.log'.format(source_filename, target)

            logger.info('Uploading model file [{}] and log receipt [{}] to s3'.format(target, log_filename))
            sync_log_to_s3(logger)
            log_file = open('retrain_output.log', 'r').read()
            save_model(target, 'retrain_output.log', log_filename)
            set_model(version_number)
            clear_log_file_from_s3()
            return log_file

        except Exception as ex:
            logger.error(ex)
            log_file = open('retrain_output.log', 'r').read()
            abort(message=log_file)

@api.route('/retrain')
class Retrain(Resource):
    def post(self):
        json_data = request.get_json(force=True)
        if type(json_data) == str:
            json_data = json.loads(json_data)
        if 'input_file' not in json_data:
            abort(message='Input json requires an input_file key which should match a json file in the twc-input bucket')
        test = False
        if 'test' in json_data:
            test = json_data.get('test')
        run_retrain(json_data['input_file'], json_data.get('model_name'), test=test)
        return 'Running model in async mode'

@api.route('/retrain-log')
class RetrainLog(Resource):
    def get(self):
        return get_training_log_json()
