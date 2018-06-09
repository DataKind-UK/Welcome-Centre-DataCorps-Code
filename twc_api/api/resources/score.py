import json

from flask import request
from flask_restplus import Resource
from api import api
from api.utils.models import TWCModel
from api.utils.transformers import ParseJSONToTablesTransformer
import os

@api.route('/score')
class Score(Resource):
    def __init__(self, api, *args, **kwargs):
        self.model = TWCModel()
        self.model.load(os.getenv('MODEL_PATH', 'etmodel.p'))
        self.parser = ParseJSONToTablesTransformer()
        super().__init__(api, *args, **kwargs)

    def post(self):
        json_data = request.get_json(force=True)
        if type(json_data) == str:
            json_data = json.loads(json_data)
        tables = self.parser.transform(json_data)
        return self.model.predict(tables)[0]
