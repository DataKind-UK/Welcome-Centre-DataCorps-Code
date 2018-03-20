from unittest import TestCase
from api.utils.transformers import *

consolidate = ConsolidateTablesTransformer()
add_time_features = AddTimeFeaturesTransformer()
split_current_and_ever = SplitCurrentAndEverTransformer(['ClientIssue_'])
align = AlignFeaturesToColumnSchemaTransformer()

data_gen = TrainingDataGenerator('../Welcome-Centre-DataCorps-Data/ClientDatabaseStructure.mdb.sqlite')
data = data_gen.get_training_data(1000)
limited_data = data.copy()
limited_data['Referral'] = limited_data['Referral'][limited_data['Referral']['ClientId'] == 287]

import pickle

class TestTransformer(TestCase):
    def setUp(self):
        self.transformer = TransformerPipeline([
            consolidate,
            add_time_features,
            split_current_and_ever,
        ], align)

    def test_fit_transform_equals_transform(self):
        X, y, referral_table = self.transformer.fit_transform(data)
        sample = X[referral_table['Client_ClientId'] == 287]
        with open('tmp.pkl', 'wb') as file:
            pickle.dump(self.transformer, file)

        with open('tmp.pkl', 'rb') as file:
            transformer = pickle.load(file)

        sample2, _, _ = transformer.transform(limited_data)

        self.assertTrue((sample.fillna(0).values == sample2.fillna(0).values).all())

    def test_split_transformer(self):
        X, y, referral_table = self.transformer.fit_transform(data)
        self.assertGreater(X.filter(like='_Current').shape[1], 0)
        self.assertGreater(X.filter(like='_Ever').shape[1], 0)