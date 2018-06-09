import json

from flask import request, jsonify
from flask_restplus import Resource
from api import api
from api.utils.aws import get_models, get_status, set_model

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

    def put(self):
        json_data = request.get_json(force=True)
        if type(json_data) == str:
            json_data = json.loads(json_data)
        version = json_data.get('version')
        if version is not None:
            set_model(version)