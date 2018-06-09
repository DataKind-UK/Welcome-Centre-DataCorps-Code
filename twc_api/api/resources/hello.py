from api import api
from flask_restplus import Resource


@api.route('/hello')
class HelloWorld(Resource):
    def get(self):
        return {'hey': 'jimbo'}