import json
from unittest import TestCase

import api
from api.utils.aws import save_model

request = json.load(open('../request.json', 'r'))

test_version = 10129412985

class TestCricketAPI(TestCase):

    def setUp(self):
        # api.app.app.config['TESTING'] = True
        # api.app.app.config['WTF_CSRF_ENABLED'] = False
        # api.app.app.config['DEBUG'] = False
        # api.app.app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///..' + \
        #                 os.path.join(TEST_DB)
        api.app.config.from_object('api.config.TestingConfig')
        self.app = api.app.test_client()


    def test_score(self):
        response = self.app.post('/score',
                       data=json.dumps(request),
                       content_type='application/json')

        print('hi')