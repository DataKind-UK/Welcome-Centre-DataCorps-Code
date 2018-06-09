import pandas as pd
from tqdm import tqdm
from datetime import datetime

def add_time_since_last_referral(referrals):
    """Add a time since last referral"""
    referrals['time_since'] = referrals.groupby('ClientId')['ReferralTakenDate'].diff().dt.days
    referrals['time_since'] = (1 / referrals['time_since']).fillna(0)
    return referrals


def add_a_first_referral_dummy_variable(referrals):
    """Add a first dummy variable"""
    referrals['first'] = (referrals['referral_no'] == 1).astype(int)
    return referrals


def calc_look_ahead_stats(referrals, window=365, break_length=28, break_coefficient=1):
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


def calculate_burst_number(referrals_df, break_length=28):
    referrals_df = referrals_df.sort_values('ReferralTakenDate')
    referrals_df['day_diff'] = (referrals_df.groupby('ClientId')['ReferralTakenDate']
                                .diff().dt.days)
    referrals_df['start_of_burst'] = referrals_df['day_diff'] > break_length
    referrals_df['burst_number'] = referrals_df.groupby('ClientId')['start_of_burst'].cumsum() + 1
    referrals_df['burst_length'] = (referrals_df.groupby(['ClientId', 'burst_number'])['burst_number']
                                    .transform(lambda x: x.count()))
    referrals_df['index_in_burst'] = (referrals_df.groupby(['ClientId', 'burst_number'])['ReferralTakenDate']
                                      .rank())
    referrals_df['has_had_previous_burst'] = 1 * (referrals_df['burst_number'] > 1)
    return referrals_df


def get_current_referral_issues(referrals):
    referral_reasons = referrals.filter(like='ReferralDomestic').add_prefix('reasons_')
    referral_document = referrals.filter(like='ReferralDocument').add_prefix('documents_')
    referral_benefit = referrals.filter(like='ReferralBenefit').add_prefix('benefit_')
    referral_issue = referrals.filter(like='ReferralIssue').add_prefix('r_issue_')
    referral_reason = referrals.filter(like='ReferralReason').add_prefix('reason_')
    client_issue = referrals.filter(like='ClientIssue').add_prefix('c_issue_')
    referral_agency = pd.get_dummies(referrals['ReferralAgencyId']).add_prefix('agency_')

    X = pd.concat([
        referral_reasons,
        referral_document,
        referral_benefit,
        referral_issue,
        referral_reason,
        referral_agency,
        client_issue
    ], axis=1).fillna(False).astype(bool)

    return X


def get_client_features(clients):
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


def get_feature_matrix(referrals, clients):
    general = referrals[['DependantNumber', 'LivingWithPartner']]
    current_issues = get_current_referral_issues(referrals)
    any_issue = current_issues.groupby(referrals['ClientId'], as_index=False, sort=False).expanding().sum() > 0
    any_issue.index = any_issue.index.droplevel(0)
    any_issue = any_issue.loc[referrals.index]
    referral_issues = pd.concat([general, current_issues.add_prefix('current_'),
                                 any_issue.add_prefix('ever_')], axis=1)
    client_issues = get_client_features(clients).loc[referrals['ClientId']]
    client_issues.index = referrals.index
    return pd.concat([referral_issues, client_issues], axis=1)