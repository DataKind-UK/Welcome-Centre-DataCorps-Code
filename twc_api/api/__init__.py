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

def update_model():
    current_model_key = get_current_model_key()
    model_dict = load_model_into_memory(current_model_key)
    model = TWCModel()
    model.load_from_object(model_dict['model'])
    return model

class ModelContainer(object):
    current_model = None

with app.app_context():
    ModelContainer.current_model = update_model()

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
        return ModelContainer.current_model.predict(tables)[0]