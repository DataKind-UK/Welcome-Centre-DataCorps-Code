import pandas as pd
import numpy as np
from scipy.stats import spearmanr


def get_scores_per_window(x, y, group, threshold=0.10):
    corr = spearmanr(x, y)[0]
    mu_a = x.groupby(group).mean()
    mu_p = y.groupby(group).mean()
    mu_a_top = mu_a[mu_a.rank(ascending=False) / len(mu_a) < threshold]
    mu_p_top = mu_p[mu_p.rank(ascending=False) / len(mu_p) < threshold]
    overlap = mu_p_top.index.isin(mu_a_top.index).mean()
    return pd.Series([corr, overlap], index=['spearman', 'overlap'])

def evaluate_average_weekly_rank_correlation(test_referral_table, y_test, y_pred):
    grouped_y = test_referral_table.assign(y=y_test, pred=y_pred).set_index('Referral_ReferralTakenDate')\
        .groupby([pd.Grouper(freq='1W'), 'Client_ClientId'])['y'].mean()
    grouped_pred_y = test_referral_table.assign(y=y_test, pred=y_pred).set_index('Referral_ReferralTakenDate')\
        .groupby([pd.Grouper(freq='1W'), 'Client_ClientId'])['pred'].mean()
    time_grouped = pd.concat([grouped_y, grouped_pred_y], axis=1)
    return time_grouped.reset_index().groupby(['Referral_ReferralTakenDate']).\
        apply(lambda k: get_scores_per_window(k['y'], k['pred'], k['Client_ClientId'])).dropna().mean()