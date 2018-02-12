import sqlite3
from unittest import TestCase
import pandas as pd

from api.utils.transformer import Transformer

sql_dict = {'Referral': """SELECT * FROM Referral;""",

'Client': """SELECT * FROM Client;""",

'ReferralBenefit':"""SELECT ref_dim.* FROM ReferralBenefit as ref_dim
LEFT JOIN  Referral on Referral.ReferralInstanceId = ref_dim.ReferralInstanceId;""",

'ReferralDietaryRequirements':"""SELECT ref_dim.* FROM ReferralDietaryRequirements as ref_dim
LEFT JOIN  Referral on Referral.ReferralInstanceId = ref_dim.ReferralInstanceId;""",

'ReferralDocument':"""SELECT ref_dim.* FROM ReferralDocument as ref_dim
LEFT JOIN  Referral on Referral.ReferralInstanceId = ref_dim.ReferralInstanceId;""",

'ReferralDomesticCircumstances': """SELECT ref_dim.* FROM ReferralDomesticCircumstances as ref_dim
LEFT JOIN  Referral on Referral.ReferralInstanceId = ref_dim.ReferralInstanceId;""",

'ReferralIssue':"""SELECT ref_dim.* FROM ReferralIssue as ref_dim
LEFT JOIN  Referral on Referral.ReferralInstanceId = ref_dim.ReferralInstanceId;""",

'ReferralReason': """SELECT ref_dim.* FROM ReferralReason as ref_dim
LEFT JOIN  Referral on Referral.ReferralInstanceId = ref_dim.ReferralInstanceId;""",

'ClientIssue': """SELECT * FROM ClientIssue;"""
}

path = '../../../Welcome-Centre-DataCorps-Data/ClientDatabaseStructure.mdb.sqlite'
con = sqlite3.connect(path)

def get_training_data():
    tables = {k: pd.read_sql(v, con=con) for k, v in sql_dict.items()}
    tables['Referral'] = tables['Referral'][tables['Referral']['ReferralInstanceId'] < 200]
    return tables


class TestTransformer(TestCase):
    def setUp(self):
        self.transformer = Transformer()

    def test_transformer_fit_transform(self):
        test_data = get_training_data()
        X = self.transformer.fit_transform(test_data)
        self.assertGreater(X.shape[1], 200)
        self.transformer.column_schema = self.transformer.column_schema[:200]

        test_data = get_training_data()
        X = self.transformer.transform(test_data)
        self.assertTrue((X.columns == self.transformer.column_schema).all())
