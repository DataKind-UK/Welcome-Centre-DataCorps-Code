import pandas as pd

from api.utils.feature_utils import get_feature_matrix, calc_look_ahead_stats

column_mapping = {
    'ReferralIssue': ['ReferralInstanceID', 'ClientIssueID'],
    'ReferralBenefit': ('ReferralInstanceId', 'BenefitTypeId'),
    'ReferralReason': ('ReferralInstanceID', 'ReferralReasonID'),
    'ReferralDietaryRequirements': ('ReferralInstanceID', 'DietaryRequirementsID'),
    'ReferralDomesticCircumstances': ('ReferralInstanceID', 'DomesticCircumstancesID'),
    'ReferralDocument': ('ReferralInstanceId','ReferralDocumentId'),
    "ClientIssue":"ClientIssueID"
}

class Transformer(object):
    def __init__(self, column_schema=None):
        self.column_schema = column_schema

    def fit_transform(self, tables):
        X, y = self.transform(tables)
        self.column_schema = list(X.columns)
        return X, y

    def transform(self, tables, calc_y=True):
        client_table = tables.pop('Client')
        referral_table = tables.pop('Referral').set_index('ReferralInstanceId')
        referral_table['ReferralTakenDate'] = pd.to_datetime(referral_table['ReferralTakenDate'])
        client_issue = tables.pop('ClientIssue')

        client_table = pd.concat([client_table, client_issue.groupby(['ClientId', 'ClientIssueId']).size().unstack()],
                                 axis=1)

        flat_tables = {}
        for t in tables:
            print(t)
            if len(tables[t]) > 0:
                flat_tables[t] = tables[t].groupby(column_mapping[t]).size().unstack().add_prefix(t + '_')


        for t in flat_tables:
            referral_table = referral_table.merge(flat_tables[t], left_index=True, right_index=True, how='left')


        X = get_feature_matrix(referral_table, client_table).fillna(0)
        if self.column_schema is not None:
            X = X.loc[:, self.column_schema].fillna(0)

        if calc_y:
            referrals_labelled = referral_table.reset_index().pipe(calc_look_ahead_stats, window=365, break_length=28,
                                                                   break_coefficient=1)
            y = referrals_labelled['future_referral_score'].fillna(0)
            return X, y
        else:
            return X