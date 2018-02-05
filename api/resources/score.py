from flask import request
from flask_restplus import Resource
from api import api

@api.route('/score')
class Score(Resource):
    def post(self):
        json_data = request.get_json(force=True)
        return json_data