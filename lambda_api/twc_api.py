from flask import Flask
from flask_restplus import Resource, Api
import pickle
import boto3
from io import BytesIO

app = Flask(__name__)
api = Api(app)

@api.route('/hello')
class HelloWorld(Resource):
    def get(self):
        return {'hey': 'jimbo'}

if __name__ == '__main__':
    app.run(debug=True)