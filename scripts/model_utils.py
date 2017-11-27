import pandas as pd
import numpy as np
from scipy.stats import beta

# Scoring functions
def get_bayes_ratio_by_client(referrals, break_length=28, break_coefficient=1, min_beta_fit_days=365):
    """Takes the referrals dataframe and returns a dataframe with one row per client with 
    the Empirical Bayes Ratio of their referrals"""
    gaps = (referrals.sort_values('ReferralTakenDate').groupby('ClientId')['ReferralTakenDate'].diff().dt.days > break_length).groupby(referrals['ClientId']).sum()
    days_active = (referrals['ReferralTakenDate'].max() - referrals.groupby('ClientId')['ReferralTakenDate'].min()).dt.days + 7
    weeks_active = days_active / 7
    referrals = (referrals.groupby('ClientId').size())
    simple_ratio = (referrals - gaps) / weeks_active
    a, b, loc, scale = beta.fit((simple_ratio[days_active > min_beta_fit_days]).values)
    adjusted_ratio = ((referrals - gaps * break_coefficient + a) / (weeks_active + a + b)).sort_values()
    score_df = pd.concat([referrals, gaps, weeks_active, simple_ratio, adjusted_ratio], axis=1)
    score_df.columns=['Referrals', 'Gaps', 'Weeks Active', 'Simple Ratio', 'Empirical Bayes Ratio']
    return score_df.sort_values('Empirical Bayes Ratio')


# Functions to add features (should take referral data frame 
# and return the same with addtional columns)

def add_referral_order_index(referrals):
    """Add Referral Order Index per Client (e.g. 2 is the second referral for that client)"""
    referral_no = referrals.assign(count=1).groupby('ClientId').expanding()['count'].sum()
    referral_no = referral_no.reset_index().set_index('ReferralInstanceId').drop('ClientId', axis=1)
    referrals['referral_no'] = referral_no
    return referrals

def add_time_since_last_referral(referrals):
    """Add a time since last referral"""
    referrals['time_since'] = referrals.groupby('ClientId')['ReferralTakenDate'].diff().dt.days
    referrals['time_since'] = (1 / referrals['time_since']).fillna(0)
    return referrals

def add_a_first_referral_dummy_variable(referrals):
    """Add a first dummy variable"""
    referrals['first'] = (referrals['referral_no'] == 1).astype(int)
    return referrals
