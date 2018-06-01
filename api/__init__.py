from flask import Flask
from flask_restplus import Api



app = Flask(__name__)
app.config.from_object('api.config.ProdConfig')
api = Api(app)

from api.resources.score import Score
from api.resources.retrain import Retrain
from api.resources.models import Status, Models