import json
import pickle
import unittest
from unittest import TestCase
import unittest.mock as mock
import requests
import api
import os
import pandas as pd

test_json = json.load(open('../../../requests/JSONExport20180819140915.JSON', 'r'))

def get_test_payloads():
    test_users = pickle.load(open('test_clients.p', 'rb'))
    payloads = []
    for t in test_json:
        if t['client'][0]['clientid'] in test_users:
            payloads.append(t)
    return payloads

test_payloads = get_test_payloads()

class TestRetrain(TestCase):
    def setUp(self):
        # api.app.app.config['TESTING'] = True
        # api.app.app.config['WTF_CSRF_ENABLED'] = False
        # api.app.app.config['DEBUG'] = False
        # api.app.app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///..' + \
        #                 os.path.join(TEST_DB)
        api.app.config.from_object('api.config.TestingConfig')
        self.app = api.app.test_client()

    @mock.patch('api.get_current_model_key', autospec=True)
    @mock.patch('api.load_model_into_memory', autospec=True)
    @mock.patch('api.save_model', autospec=True)
    @mock.patch('api.load_train_file_into_memory', autospec=True)
    @mock.patch('api.next_model_name', autospec=True)
    def test_retrain(self, f1, f2, f3, f4, f5):
        f1.return_value = 'twc_model_5'
        f2.return_value = test_json
        f5.return_value = 'twc_model_5'

        self.app.post('retrain', json={'input_file': 'blah', 'test': True})
        predictions = pickle.load(open('test_predictions.p', 'rb'))
        f4.return_value = pickle.load(open('twc_model_5', 'rb'))
        # for t in test_payloads:
        #     response = self.app.post('score', json=t)
        #     print('hi')

    @mock.patch('api.get_current_model_key', autospec=True)
    @mock.patch('api.load_model_into_memory', autospec=True)
    def test_equivalence(self, f1, f2):
        predictions = pickle.load(open('test_predictions.p', 'rb'))
        f1.return_value = pickle.load(open('twc_model_5', 'rb'))

        for i, t in enumerate(test_payloads[248:]):
            response = self.app.post('score', json=t)
            return_series = pd.Series(response.json)
            return_series.index = return_series.index.astype(int)
            df = pd.concat([predictions.loc[return_series.index], return_series], axis=1).dropna()
            all_matched = ((df[1] - df[0]).abs() < 1e-9).all()
            print(all_matched)
            # self.assertTrue(all_matched)
