import pandas as pd
from tqdm import tqdm
from datetime import datetime

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

    def get_client_features(self, clients):
        clients['ClientDateOfBirth'] = pd.to_datetime(clients['ClientDateOfBirth'])
        clients['AddressSinceDate'] = pd.to_datetime(clients['AddressSinceDate'])
        clients['Age'] = datetime.now() - clients['ClientDateOfBirth']
        clients['Age'] = clients['Age'].dt.days / 365
        clients.loc[clients['Age'] < 0, 'Age'] += 100
        clients['AddressLength'] = (datetime.now() - clients['AddressSinceDate']).dt.days / 365
        categories = pd.get_dummies(clients[['ClientEthnicityID', 'ClientCountryID',
                                             'ClientAddressTypeID', 'AddressPostCode',
                                             'AddressLocalityId', 'ClientResidencyId']].astype(str))
        clients['known_partner'] = clients['PartnerId'].notnull()

        client_features = pd.concat([clients[['Age', 'AddressLength', 'ClientIsMale', 'known_partner']],
                                     categories], axis=1)
        client_features = client_features.fillna(client_features.median())
        return client_features

    def get_feature_matrix(self, referrals, clients):
        general = referrals[['DependantNumber', 'LivingWithPartner']]
        current_issues = self.get_current_referral_issues(referrals)
        any_issue = current_issues.groupby(referrals['ClientId'], as_index=False, sort=False).expanding().sum() > 0
        any_issue.index = any_issue.index.droplevel(0)
        any_issue = any_issue.loc[referrals.index]
        referral_issues = pd.concat([general, current_issues.add_prefix('current_'),
                                     any_issue.add_prefix('ever_')], axis=1)
        client_issues = self.get_client_features(clients).loc[referrals['ClientId']]
        client_issues.index = referrals.index
        return pd.concat([referral_issues, client_issues], axis=1)

    def calc_look_ahead_stats(self, referrals, window=365, break_length=28, break_coefficient=1):
        all_ratios = []
        referral_no = referrals.assign(count=1).groupby('ClientId').expanding()['count'].sum()
        referral_no = referral_no.reset_index().set_index('level_1')['count']
        referrals['referral_no'] = referral_no.loc[referrals.index]
        for i in tqdm(range(1, int(referral_no.max()))):
            # Grab the segment for each no of referrals
            segment = referrals.loc[referral_no == i, :]
            reference_date = segment.set_index('ClientId')['ReferralTakenDate']
            referrals = referrals.assign(reference_date=reference_date.loc[referrals.ClientId].values)
            date_diff = (referrals['ReferralTakenDate'] - referrals['reference_date']).dt.days
            year_range = referrals[(date_diff >= 0) & (date_diff <= window)]

            gaps = ((year_range.sort_values('ReferralTakenDate')
                     .groupby('ClientId')['ReferralTakenDate']
                     .diff().dt.days > break_length)
                    .groupby(year_range['ClientId']).sum())
            counts = (year_range.groupby('ClientId').size()) - 1
            future_referral_score = (counts - gaps * break_coefficient) / (window / 7)
            segment_ratios = pd.concat([counts, future_referral_score, gaps],
                                       axis=1).loc[segment.ClientId]
            segment_ratios.columns = ['counts', 'future_referral_score', 'gaps']
            segment_ratios.index = segment.index
            all_ratios.append(segment_ratios)
        all_ratios_df = pd.concat(all_ratios).loc[referrals.index]
        referrals[all_ratios_df.columns] = all_ratios_df
        return referrals

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


        X = self.get_feature_matrix(referral_table, client_table).fillna(0)
        if self.column_schema is not None:
            X = X.loc[:, self.column_schema].fillna(0)

        if calc_y:
            referrals_labelled = referral_table.reset_index().pipe(self.calc_look_ahead_stats,
                                                                   window=365, break_length=28,
                                                                   break_coefficient=1)
            y = referrals_labelled['future_referral_score'].fillna(0)
            return X, y
        else:
            return X