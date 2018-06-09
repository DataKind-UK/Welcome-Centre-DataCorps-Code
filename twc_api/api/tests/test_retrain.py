import json
import unittest
from unittest import TestCase
import requests
import api
import os


class TestRetrain(TestCase):

    def test_retrain(self):
        json_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        with open(os.path.join(json_path, 'requestOldData.json'), 'r') as f:
            self.json_data = json.load(f)
        self.url = 'http://localhost:8080/retrain'
        r = requests.post(self.url, json=self.json_data)
        self.assertTrue(r.status_code == 200)

if __name__ == '__main__':
    unittest.main()