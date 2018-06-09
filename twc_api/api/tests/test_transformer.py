from unittest import TestCase
from api.utils.transformers import *
import os
consolidate = ConsolidateTablesTransformer(count_encode=False)
add_target_features = AddFutureReferralTargetFeatures()
add_time_window_features = TimeWindowFeatures(windows=[10, 2])
split_current_and_ever = SplitCurrentAndEverTransformer(['ClientIssue_'])
align = AlignFeaturesToColumnSchemaTransformer()

db_path = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                       '..', '..', '..', 'Welcome-Centre-DataCorps-Data', 'ClientDatabaseStructure.mdb.sqlite')

data_gen = TrainingDataGenerator(db_path)
data = data_gen.get_training_data(1000)
limited_data = data.copy()
limited_data['Referral'] = limited_data['Referral'][limited_data['Referral']['ClientId'] == 287]

import pickle

class TestTransformer(TestCase):
    def setUp(self):
        self.transformer = TransformerPipeline([
            consolidate,
            add_target_features,
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

class TestCountEncoding(TestCase):
    def test_count_encoder_reduces_dims(self):
        transformer = TransformerPipeline([
            ConsolidateTablesTransformer(count_encode=False),
            add_target_features
        ], align)
        X, _, _ = transformer.fit_transform(data)
        one_hot_shape = X.shape[1]
        print(X.shape)
        transformer = TransformerPipeline([
            ConsolidateTablesTransformer(count_encode=True),
            add_target_features
        ], align)
        X, _, _ = transformer.fit_transform(data)
        print(X.shape)
        encoded_shape = X.shape[1]
        self.assertGreater(one_hot_shape, encoded_shape)


class TestTimeWindowFeatures(TestCase):
    def setUp(self):
        self.transformer = TransformerPipeline([
            consolidate,
            add_target_features,
            add_time_window_features
        ], align)

    def test_time_window_settings(self):
        X, y, referral_table = self.transformer.fit_transform(data)
        self.assertEqual(X.filter(like='window_count').shape[1], 2)
        self.assertTrue((X['window_count_2'] <= X['window_count_10']).all())
