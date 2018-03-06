from sklearn.ensemble import RandomForestRegressor
from api.utils.transformers import FullTransformer
import pickle

class TWCModel(object):
    def __init__(self):
        self.transformer = None
        self.model = None

    def get_model(self):
        return RandomForestRegressor(n_jobs=-1, n_estimators=1000)

    def fit(self, tables):
        self.transformer = FullTransformer()
        self.model = self.get_model()
        X, y = self.transformer.fit_transform(tables)
        self.model.fit(X, y)

    def predict(self, tables):
        X = self.transformer.transform(tables)
        return self.model.predict(X)

    def save(self, file_name):
        with open(file_name, 'wb') as file:
            pickle.dump({'transformer':self.transformer,
                        'model': self.model}, file)

    def load(self, file_name):
        with open(file_name, 'rb') as file:
            manifest = pickle.load(file)
            self.transformer = manifest['transformer']
            self.model = manifest['model']
