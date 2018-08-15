from scipy.stats import spearmanr
import pandas as pd

def get_scores_per_window(actual, predicted, group, threshold=0.50):
    corr = spearmanr(actual, predicted)[0]
    mu_a = actual.groupby(group).mean()
    mu_p = predicted.groupby(group).mean()
    mu_a_top = mu_a[mu_a.rank(ascending=False) / len(mu_a) < threshold]
    mu_p_top = mu_p[mu_p.rank(ascending=False) / len(mu_p) < threshold]
    overlap = mu_p_top.index.isin(mu_a_top.index).mean()
    return pd.Series([corr, overlap], index=['spearman', 'overlap'])

def evaluate_average_weekly_rank_correlation(test_referral_table, y_test, y_pred, threshold):
    grouped_y = test_referral_table.assign(y=y_test, pred=y_pred)\
                                    .set_index('referral_referraltakendate')\
                                    .groupby([pd.Grouper(freq='1W'), 'client_clientid'])['y']\
                                    .mean()
    grouped_pred_y = test_referral_table.assign(y=y_test, pred=y_pred)\
                                        .set_index('referral_referraltakendate')\
                                        .groupby([pd.Grouper(freq='1W'), 'client_clientid'])['pred']\
                                        .mean()
    time_grouped = pd.concat([grouped_y, grouped_pred_y], axis=1)
    return time_grouped.reset_index()\
                .groupby(['referral_referraltakendate'])\
                .apply(lambda k: get_scores_per_window(k['y'], k['pred'], k['client_clientid'], threshold)).dropna().mean()