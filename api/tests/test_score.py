import json
import unittest
from unittest import TestCase
import requests
import api
import os


class TestScore(TestCase):

    def test_score(self):
        json_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        with open(os.path.join(json_path, 'request1872.json'), 'r') as f:
            self.json_data = json.load(f)
        self.url = 'http://localhost:8080/score'
        r = requests.post(self.url, json=self.json_data)
        print(r.json())
        self.assertTrue(r.status_code == 200)

if __name__ == '__main__':
    unittest.main()