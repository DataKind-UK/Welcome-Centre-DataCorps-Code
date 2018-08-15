from sklearn.ensemble import RandomForestRegressor
from api.model.transformers import *
import pickle

class TWCModel(object):
    def __init__(self, transformer, model):
        self.transformer = transformer
        self.model = model

    def get_model(self):
        return RandomForestRegressor(n_jobs=-1, n_estimators=150)

    def predict(self, tables):
        X, _, _ = self.transformer.transform(tables)
        return self.model.predict(X)

    def save(self, file_name):
        with open(file_name, 'wb') as file:
            pickle.dump({'transformer': self.transformer,
                        'model': self.model}, file)

    def load_from_file(self, file_name):
        with open(file_name, 'rb') as file:
            manifest = pickle.load(file)
            self.transformer = manifest['transformer']
            self.model = manifest['model']

    def load_from_object(self, model_object):
        self.transformer = model_object.transformer
        self.model = model_object.model

if __name__ == '__main__':
    data_gen = TrainingDataGenerator('../../../Welcome-Centre-DataCorps-Data/ClientDatabaseStructure.mdb.sqlite')
    data = data_gen.get_training_data()
    model = TWCModel()
    model.fit(data)
    model.save('model.p')