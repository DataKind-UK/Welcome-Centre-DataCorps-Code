from flask import Flask
from flask import request
from flask_restplus import Api, Resource
from api.utils.aws import (get_models, set_model, get_status,
                           get_current_model_key, load_model_into_memory)
from api.utils.models import TWCModel
from api.utils.transformers import ParseJSONToTablesTransformer
import json
import logging

app = Flask(__name__)
app.config.from_object('api.config.ProdConfig')
api = Api(app, title='The Welcome Centre - DataKind API', default='Endpoints', default_label='')

logging.getLogger('s3transfer').setLevel(logging.CRITICAL)

class ModelContainer(object):
    current_model = None
    current_version = None

def update_model():
    current_model_key = get_current_model_key()
    if ModelContainer.current_model != current_model_key:
        app.logger.info('Model state changed, loading model')
        model_dict = load_model_into_memory(current_model_key)
        model = TWCModel()
        model.load_from_object(model_dict['model'])
        ModelContainer.current_model, ModelContainer.current_version = model, current_model_key

@api.route('/set-model')
@api.doc(description='Sets the live model version in the API',
         params={'version':'Version number of the model.'})
class SetModel(Resource):
    def get(self):
        version = request.args.get('version')
        set_model(version)
        ModelContainer.current_model = update_model()
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
        return ModelContainer.current_model.predict(tables)[0]

def run_retrain(source, target):
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

    EC2 = boto3.client('ec2', region_name='eu-west-1')
    instance = EC2.run_instances(
        InstanceType='t2.micro',
        ImageId='ami-ca0135b3',
        MinCount=1,  # required by boto, even though it's kinda obvious.
        MaxCount=1,
        InstanceInitiatedShutdownBehavior='terminate',  # make shutdown in script terminate ec2
        IamInstanceProfile={'Name': 'twc_retrain'},
        UserData=init_script.format(target, source)  # file to run on instance init.
    )

    instance_id = instance['Instances'][0]['InstanceId']

    return instance_id

@api.route('/retrain')
class Retrain(Resource):
    def post(self):
        json_data = request.get_json(force=True)
        if type(json_data) == str:
            json_data = json.loads(json_data)
        return run_retrain(json_data['input_file'], json_data['model_name'])