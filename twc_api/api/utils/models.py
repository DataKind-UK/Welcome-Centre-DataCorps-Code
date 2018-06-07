from sklearn.ensemble import RandomForestRegressor
from api.utils.transformers import *
import pickle

class TWCModel(object):
    def __init__(self):
        self.transformer = None
        self.model = None

    def get_model(self):
        return RandomForestRegressor(n_jobs=-1, n_estimators=1000)

    def fit(self, tables):
        self.transformer = TransformerPipeline([
            ConsolidateTablesTransformer(),
            AddTimeFeaturesTransformer(),
            SplitCurrentAndEverTransformer(['ReferralIssue_', 'ReferralDomesticCircumstances_',
                                            'ReferralReason_', 'ReferralBenefit_'])
        ], aligner=AlignFeaturesToColumnSchemaTransformer())

        self.model = self.get_model()
        X, y, _ = self.transformer.fit_transform(tables)
        self.model.fit(X, y)

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