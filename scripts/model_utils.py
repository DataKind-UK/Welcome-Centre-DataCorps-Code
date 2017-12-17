import pandas as pd
import numpy as np
from scipy.stats import beta
from tqdm import tqdm_notebook


# Functions to add features (should take referral data frame 
# and return the same with addtional columns)

def add_time_since_last_referral(referrals):
    """Add a time since last referral"""
    referrals['time_since'] = referrals.groupby('ClientId')['ReferralTakenDate'].diff().dt.days
    referrals['time_since'] = (1 / referrals['time_since']).fillna(0)
    return referrals

def add_a_first_referral_dummy_variable(referrals):
    """Add a first dummy variable"""
    referrals['first'] = (referrals['referral_no'] == 1).astype(int)
    return referrals

def calc_look_ahead_stats(referrals, window=365, break_length=28, break_coefficient=1, min_beta_fit_days=90):
    all_ratios = []
    referral_no = referrals.assign(count=1).groupby('ClientId').expanding()['count'].sum()
    referral_no = referral_no.reset_index().set_index('level_1')['count']
    referrals['referral_no'] = referral_no.loc[referrals.index]
    for i in tqdm_notebook(range(1, int(referral_no.max()))):
        # Grab the segment for each no of referrals
        segment = referrals.loc[referral_no==i,:]
        reference_date = segment.set_index('ClientId')['ReferralTakenDate']
        referrals = referrals.assign(reference_date=reference_date.loc[referrals.ClientId].values)
        date_diff = (referrals['ReferralTakenDate']-referrals['reference_date']).dt.days
        year_range = referrals[(date_diff > 0) & (date_diff <= window)]
        
        gaps = (year_range.sort_values('ReferralTakenDate').groupby('ClientId')['ReferralTakenDate'].diff().dt.days > break_length).groupby(year_range['ClientId']).sum()
        days_active = ((year_range['ReferralTakenDate'].max() - year_range.groupby('ClientId')['ReferralTakenDate'].min()).dt.days + 7).clip(0, window)
        weeks_active = days_active / 7
        counts = (year_range.groupby('ClientId').size())
        simple_ratio = (counts - gaps * break_coefficient) / weeks_active
        segment_ratios = pd.concat([counts, simple_ratio, days_active, gaps, weeks_active], axis=1).loc[segment.ClientId]
        segment_ratios.columns = ['counts', 'simple', 'days', 'gaps', 'weeks']
        segment_ratios.index = segment.index
        # Fill in details for last referral per client
        segment_ratios['counts'] = segment_ratios['counts'].fillna(0)
        segment_ratios['gaps'] = segment_ratios['gaps'].fillna(0)
        segment_ratios['days'] = segment_ratios['days'].fillna(((segment['ReferralTakenDate'].max() 
                                   - segment['ReferralTakenDate']).dt.days + 7).clip(0, window))
        segment_ratios['weeks'] = segment_ratios['weeks'].fillna(segment_ratios['days']/7)
        segment_ratios['simple'] = segment_ratios['simple'].fillna(0)
        all_ratios.append(segment_ratios)
    all_ratios_df = pd.concat(all_ratios).loc[referrals.index]
    
    # Calculate Beta coefficients
    a, b, loc, scale = beta.fit(all_ratios_df[all_ratios_df['days'] > min_beta_fit_days]['simple'].values)
    print('Beta parameters alpha: {}, beta: {}'.format(a,b))

    all_ratios_df['adjusted_ratio'] = ((all_ratios_df['counts'] - all_ratios_df['gaps'] 
                       * break_coefficient + a) / (all_ratios_df['weeks'] + a + b))
    
    referrals[all_ratios_df.columns] = all_ratios_df
    return referrals


