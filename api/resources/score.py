from flask import request
from flask_restplus import Resource
from api import api
from api.utils.model import TWCModel
from api.utils.parser import parse_json_reponse_into_df_dict
import os

@api.route('/score')
class Score(Resource):
    def __init__(self, api, *args, **kwargs):
        self.model = TWCModel()
        self.model.load(os.getenv('MODEL_PATH', 'model.p'))
        super().__init__(api, *args, **kwargs)

    def post(self):
        json_data = request.get_json(force=True)
        tables = parse_json_reponse_into_df_dict(json_data)
        return self.model.predict(tables)[0]
