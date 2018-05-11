import json
from collections import defaultdict

from flask import request
from flask_restplus import Resource
from api import api
import pandas as pd

@api.route('/retrain')
class Retrain(Resource):
    def post(self):
        print('I received data')
        json_data = request.get_json(force=True)
        if type(json_data) == str:
            json_data = json.loads(json_data)
        construct_full_tables(json_data)
        print('hi')

def construct_full_tables(json_data):
    tables = defaultdict(list)
    for row in json_data:
        for key in row:
            tables[key].append(pd.DataFrame.from_dict(row[key]))
    return {k: pd.concat(v) for k, v in tables.items()}