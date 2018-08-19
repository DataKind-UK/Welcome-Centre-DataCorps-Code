import json
import pickle
from collections import defaultdict
from itertools import chain

from api.model.models import TWCModel
from api.utils.evaluate import evaluate_average_weekly_rank_correlation
import pandas as pd
from api.model.transformers import (TransformerPipeline, ConsolidateTablesTransformer,
                                    AddFutureReferralTargetFeatures, TimeFeatureTransformer,
                                    SplitCurrentAndEverTransformer,
                                    AlignFeaturesToColumnSchemaTransformer)
from sklearn.ensemble import ExtraTreesRegressor
import logging

table_names = ['referral', 'referralreason', 'client', 'referralbenefit', 'referralissue', 'referraldocument',
          'referraldomesticcircumstances', 'clientissue', 'referraldietaryrequirements']

logger = logging.getLogger('twc_logger')

def train_model_from_json(json_data, hyperparams=None, limit=None, test=False):
    logger.info('Beginning table parse')
    tables = construct_full_tables(json_data, limit)
    # Generate feature matrix and target vector
    X, y, referral_table, transformer = generate_X_y(tables)
    # Split train and test sets
    if test:
        X_train, X_test, y_train, y_test, referral_table_train, referral_table_test = \
            split_train_test(X, y, referral_table)
        # Evaluate  model
        new_model = train_model(X_train, y_train, hyperparams)

        evaluate_model(new_model, X_test, y_test, referral_table_test, 0.2)

        twc_model = TWCModel(transformer, new_model)

        return X, y, referral_table, twc_model
    else:
        new_model = train_model(X, y, hyperparams)
        twc_model = TWCModel(transformer, new_model)
        return X, y, referral_table, twc_model


def construct_full_tables(json_data, limit=None):
    tables = {}
    if limit is not None:
        json_data = json_data[:limit]
    for table_name in table_names:
        records = [j.get(table_name) for j in json_data if table_name in j]
        tables[table_name] = pd.DataFrame(list(chain.from_iterable(records)))
    return tables


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
    test_start = int((1 - test_proportion) * max_index)
    X_train = X.iloc[0:test_start]
    X_test = X.iloc[test_start:]
    y_train = y.iloc[0:test_start]
    y_test = y.iloc[test_start:]
    referral_table_train = referral_table.iloc[0:test_start]
    referral_table_test = referral_table.iloc[test_start:]
    logger.info("Train/Test sets split.\n" \
                "Train set: {} referrals.\n" \
                "Test set: {} referrals".format(len(X_train), len(X_test)))
    return X_train, X_test, y_train, y_test, referral_table_train, referral_table_test


def train_model(X_train, y_train, hyperparams=None):
    if hyperparams is not None:
        et = ExtraTreesRegressor(n_jobs=-1, **hyperparams)
    else:
        et = ExtraTreesRegressor(n_jobs=-1, n_estimators=120)

    et.fit(X_train, y_train)
    logger.info('Trained Model on: {} observations'.format(len(X_train)))
    return et


def evaluate_model(model, X_test, y_test, referral_table_test, threshold):
    y_pred = model.predict(X_test)
    # pickle.dump(pd.Series(y_pred, X_test.index), open('test_predictions.p', 'wb'))
    # pickle.dump(X_test, open('test_features.p', 'wb'))
    # pickle.dump(referral_table_test, open('ref_table.p', 'wb'))

    evaluation_series = evaluate_average_weekly_rank_correlation(referral_table_test,
                                                                 y_test, y_pred, threshold)
    logger.info("Model Test Evaluation Metrics:\n" \
                "\tTest Set Correlation of Predicted and Actual Mean Weekly Scores: {}\n" \
                "\tTest Set Overlap of top {}% worst cases: {}" \
                .format(evaluation_series['spearman'], threshold * 100, evaluation_series['overlap']))

