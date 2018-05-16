import json
from collections import defaultdict

from flask import request
from flask_restplus import Resource
from api import api
import pandas as pd
from api.utils.transformers import (TransformerPipeline, ConsolidateTablesTransformer,
                                    AddFutureReferralTargetFeatures, TimeFeatureTransformer,
                                    SplitCurrentAndEverTransformer,
                                    AlignFeaturesToColumnSchemaTransformer)
from sklearn.ensemble import ExtraTreesRegressor


    

@api.route('/retrain')
class Retrain(Resource):
    def post(self):
        summary_statuses = []
        # Parse JSON data
        json_data = request.get_json(force=True)
        if type(json_data) == str:
            json_data = json.loads(json_data)
        # Construct dictionary of tables
        tables = construct_full_tables(json_data)
        # Generate feature matrix and target vector
        X, y, referral_table, generation_summary = generate_X_y(tables)
        summary_statuses.append(generation_summary)
        # Split train and test sets
        X_train, X_test, y_train, y_test, split_summary = split_train_test(X, y)
        summary_statuses.append(split_summary)
        # Train model
        new_model, training_status = train_model(X_train, y_train)
        summary_statuses.append(training_status)
        print('\n'.join(summary_statuses))
        return '\n'.join(summary_statuses)

def construct_full_tables(json_data):
    tables = defaultdict(list)
    for row in json_data:
        for key in row:
            tables[key].append(pd.DataFrame.from_dict(row[key]))
    return {k: pd.concat(v) for k, v in tables.items()}

def generate_X_y(tables):
    transformer = TransformerPipeline([
                        ConsolidateTablesTransformer(count_encode=False),
                        AddFutureReferralTargetFeatures(),
                        TimeFeatureTransformer(break_length=28),
                        SplitCurrentAndEverTransformer(['ReferralIssue_', 
                                                       'ReferralDomesticCircumstances_',
                                                        'ReferralReason_', 'ReferralBenefit_'])
                                    ], aligner=AlignFeaturesToColumnSchemaTransformer())
    X, y, referral_table = transformer.fit_transform(tables)
    # Since the data is all numerical or dummied we can fill any nulls with 0
    X = X.fillna(0)
    generation_summary = "Features Matrix generated" \
    " consisting of {} referrals and {} features".format(X.shape[0], X.shape[1])
    return X, y, referral_table, generation_summary

def split_train_test(X, y, test_proportion=0.2):
    max_index = len(X) - 1
    test_start = int((1-test_proportion)*max_index)
    X_train = X.iloc[0:test_start]
    X_test = X.iloc[test_start:]
    y_train = y.iloc[0:test_start]
    y_test = y.iloc[test_start:]
    split_summary = "Train/Test sets split.\n"\
                    "Train set: {} referrals.\n"\
                    "Test set: {} referrals".format(len(X_train), len(X_test))
    return X_train, X_test, y_train, y_test, split_summary

def train_model(X_train, y_train):
    et = ExtraTreesRegressor(n_jobs=-1, n_estimators=500)
    et.fit(X_train, y_train)
    training_status = 'Trained Model'
    return et, training_status