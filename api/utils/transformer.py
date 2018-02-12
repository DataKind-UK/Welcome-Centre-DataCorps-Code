
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
    def fit_transform(self, tables):
        client_table = tables.pop('Client')
        referral_table = tables.pop('Referral').set_index('ReferralInstanceId')
        client_issue = tables.pop('ClientIssue')

        flat_tables = {t: tables[t].groupby(column_mapping[t]).size().unstack().add_prefix(t + '_') for t in
            tables}
        for t in flat_tables:
            referral_table = referral_table.merge(flat_tables[t], left_index=True, right_index=True, how='left')

        print('hi')