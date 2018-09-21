import os
import shutil
from time import sleep

from flask import Flask, redirect, url_for
from flask import request
from flask_restplus import Api, Resource, abort

from api.model.train import train_model_from_json
from api.utils.aws import (get_models, set_model, get_status, load_train_file_into_memory,
                           get_current_model_key, load_model_into_memory, next_model_name, save_model,
                           sync_log_to_s3, clear_log_file_from_s3, get_training_log_json, download_log_from_s3)
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
class CurrentModel(Resource):
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
        return {
            'model_name': get_current_model_key(),
            'scores': ModelContainer.current_model.predict(tables)
        }

def get_logger(append_previous=False):
    if append_previous:
        download_log_from_s3()
    else:
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
def run_retrain(source_filename, hyperparams=None, test=False):
    with app.app_context():  # This is used since the run_retrain requires app context 

        target, version_number = next_model_name()

        logger = get_logger(not test)

        if test:
            logger.info('Starting test evaluation from source json dump {}'.format(source_filename, target))
        else:
            logger.info('Starting full retrain from source json dump {} to model file {}'.format(source_filename, target))

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
            if test:
                run_retrain(source_filename, hyperparams, False)
            else:
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

@api.route('/retrain/<string:input_file>')
class Retrain(Resource):
    def get(self, input_file):
        run_retrain(input_file, test=True)
        sleep(5)
        return redirect(url_for('retrain_log'))

@api.route('/retrain-log')
class RetrainLog(Resource):
    def get(self):
        return get_training_log_json()
