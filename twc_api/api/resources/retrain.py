import json
from collections import defaultdict
from api.utils.evaluate import evaluate_average_weekly_rank_correlation
from flask import request
from flask_restplus import Resource
from api import api
import pandas as pd
from api.model.transformers import (TransformerPipeline, ConsolidateTablesTransformer,
                                    AddFutureReferralTargetFeatures, TimeFeatureTransformer,
                                    SplitCurrentAndEverTransformer,
                                    AlignFeaturesToColumnSchemaTransformer)
from sklearn.ensemble import ExtraTreesRegressor
import logging

logger = logging.getLogger('twc_logger')

def train_model_from_json(json_data, hyperparams=None, limit=None):
    tables = construct_full_tables(json_data, limit)
    # Generate feature matrix and target vector
    X, y, referral_table = generate_X_y(tables)
    # Split train and test sets
    X_train, X_test, y_train, y_test, referral_table_train, referral_table_test = \
        split_train_test(X, y, referral_table)
    # Evaluate  model
    new_model = train_model(X_train, y_train, hyperparams)

    evaluate_model(new_model, X_test, y_test, referral_table_test, 0.2)

    return X, y, referral_table, new_model
    #
    # # Return Threshold
    # """PLACE HOLDER FOR CREATING A THRESHOLD/REFERRALS PER WEEK CURVE"""
    #
    # # Train production model on all data
    # new_model, final_training_status = train_model(X, y)
    # summary_statuses.append(final_training_status)
    # print('\n'.join(summary_statuses))
    # return X, y, referral_table, new_model, '\n'.join(summary_statuses)


@api.route('/retrain')
class Retrain(Resource):
    def post(self):
        summary_statuses = []
        # Parse JSON data
        json_data = request.get_json(force=True)
        if type(json_data) == str:
            json_data = json.loads(json_data)
        # Construct dictionary of tables
        model, message = train_model_from_json(json_data)
        return message

def construct_full_tables(json_data, limit=None):
    tables = defaultdict(list)
    if limit is not None:
        json_data = json_data[:limit]
    for row in json_data:
        for key in row:
            tables[key].append(pd.DataFrame.from_dict(row[key]))
    return {k: pd.concat(v) for k, v in tables.items()}

def generate_X_y(tables):
    transformer = TransformerPipeline([
                        ConsolidateTablesTransformer(count_encode=False),
                        AddFutureReferralTargetFeatures(),
                        TimeFeatureTransformer(break_length=28),
                        SplitCurrentAndEverTransformer(['referralissue_', 
                                                       'referraldomesticcircumstances_',
                                                        'referralreason_', 'referralbenefit_'])
                                    ], aligner=AlignFeaturesToColumnSchemaTransformer())
    X, y, referral_table = transformer.fit_transform(tables)
    max_dt = referral_table['referral_referraltakendate'].max()
    # Need to clip the data to have at least a year of observation
    last_acceptable_date = max_dt - pd.Timedelta('365 days')

    X = X[referral_table['referral_referraltakendate'] <= last_acceptable_date]

    y = y[referral_table['referral_referraltakendate'] <= last_acceptable_date]

    referral_table = referral_table[referral_table['referral_referraltakendate'] 
                                                    <= last_acceptable_date]
                                                    
    # Since the data is all numerical or dummied we can fill any nulls with 0
    X = X.fillna(0)
    logger.info("Features Matrix generated" \
    " consisting of {} referrals and {} features".format(X.shape[0], X.shape[1]))
    return X, y, referral_table, transformer

def split_train_test(X, y, referral_table, test_proportion=0.25):
    max_index = len(X) - 1
    test_start = int((1-test_proportion)*max_index)
    X_train = X.iloc[0:test_start]
    X_test = X.iloc[test_start:]
    y_train = y.iloc[0:test_start]
    y_test = y.iloc[test_start:]
    referral_table_train = referral_table.iloc[0:test_start]
    referral_table_test = referral_table.iloc[test_start:]
    logger.info("Train/Test sets split.\n"\
                    "Train set: {} referrals.\n"\
                    "Test set: {} referrals".format(len(X_train), len(X_test)))
    return X_train, X_test, y_train, y_test, referral_table_train, referral_table_test

def train_model(X_train, y_train, hyperparams=None):
    if hyperparams is not None:
        et = ExtraTreesRegressor(n_jobs=-1, **hyperparams)
    else:
        et = ExtraTreesRegressor(n_jobs=-1, n_estimators=500)

    et.fit(X_train, y_train)
    logger.info('Trained Model on: {} observations'.format(len(X_train)))
    return et

def evaluate_model(model, X_test, y_test, referral_table_test, threshold):
    y_pred = model.predict(X_test)
    evaluation_series = evaluate_average_weekly_rank_correlation(referral_table_test,
                                                                 y_test, y_pred, threshold)
    logger.info("Model Test Evaluation Metrics:\n"\
                        "\tTest Set Correlation of Predicted and Actual Mean Weekly Scores: {}\n"\
                        "\tTest Set Overlap of top {}% worst cases: {}"\
                        .format(evaluation_series['spearman'], threshold*100, evaluation_series['overlap']))

