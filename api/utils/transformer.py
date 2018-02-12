import pandas as pd

from api.utils.feature_utils import get_feature_matrix

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
    def __init__(self):
        self.column_schema = None

    def fit_transform(self, tables):
        X = self.transform(tables)
        self.column_schema = X.columns
        return X

    def transform(self, tables):
        client_table = tables.pop('Client')
        referral_table = tables.pop('Referral').set_index('ReferralInstanceId')
        client_issue = tables.pop('ClientIssue')

        client_table = pd.concat([client_table, client_issue.groupby(['ClientId', 'ClientIssueId']).size().unstack()],
                                 axis=1)

        flat_tables = {t: tables[t].groupby(column_mapping[t]).size().unstack().add_prefix(t + '_') for t in
                       tables}
        for t in flat_tables:
            referral_table = referral_table.merge(flat_tables[t], left_index=True, right_index=True, how='left')

        X = get_feature_matrix(referral_table, client_table)
        if self.column_schema is not None:
            X = X.loc[:, self.column_schema]
        return X