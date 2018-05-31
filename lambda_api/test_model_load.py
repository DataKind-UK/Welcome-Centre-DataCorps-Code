import boto3
from io import BytesIO
import pickle


class TWCModel(object):
    def __init__(self):
        self.transformer = None
        self.model = None

    def predict(self, tables):
        X, _, _ = self.transformer.transform(tables)
        return self.model.predict(X)

    def load(self, bucket_name, key):
        s3 = boto3.resource('s3')
        with BytesIO() as data:
            s3.Bucket(bucket_name).download_fileobj(key, data)
            data.seek(0)    # move back to the beginning after writing
            manifest = pickle.load(data)
            self.transformer = manifest['transformer']
            self.model = manifest['model']

    def save(self, file_name):
        with open(file_name, 'wb') as file:
            pickle.dump({'transformer': self.transformer,
                        'model': self.model}, file)

if __name__ == '__main__':
    t = TWCModel()
    p = t.load('twc-lambda-api-test', 'etmodel.p')