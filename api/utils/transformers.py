import pandas as pd
from tqdm import tqdm
from datetime import datetime
import json
import sqlite3

class BaseTransformer(object):
    def fit_transform(self, X):
        return self.transform(X)

    def transform(self, X):
        pass

class TimeFeatureTransformer(BaseTransformer):
    def __init__(self, break_length):
        self.break_length = break_length

    def fit_transform(self, referral_table):
        # We set the start date to ensure consistent frame of reference
        # across training and live data
        self.dataset_start_date = referral_table['Referral_ReferralTakenDate'].min()
        return self.transform(referral_table)

    def transform(self, referral_table):
        valid_referrals = (referral_table[referral_table['Referral_ReferralTakenDate']
                            >= self.dataset_start_date].copy())
        valid_referrals['TimeFeature_ReferralNumber'] =( valid_referrals
                                                        .groupby('Referral_ClientId')
                                                        ['Referral_ReferralTakenDate'].rank())
        referral_table['TimeFeature_ReferralNumber'] = (valid_referrals['TimeFeature_ReferralNumber']
                                                        .loc[referral_table.index])
        # Add burst features
        referral_table = referral_table.sort_values('Referral_ReferralTakenDate')
        referral_table['TimeFeature_DaysSinceLastReferral'] = (referral_table.groupby('Referral_ClientId')
                                                    ['Referral_ReferralTakenDate'].diff().dt.days
                                                    .fillna(0))
        referral_table['TimeFeature_StartOfBurst'] = (referral_table['TimeFeature_DaysSinceLastReferral']
                                                      > self.break_length)
        referral_table['TimeFeature_BurstNumber'] = (referral_table.groupby('Referral_ClientId')
                                                    ['TimeFeature_StartOfBurst'].cumsum() + 1)
        referral_table['TimeFeature_IndexInBurst'] = (referral_table.groupby(
                                    ['Referral_ClientId', 'TimeFeature_BurstNumber'])
                                        ['Referral_ReferralTakenDate'].rank())
        ## Add a total referrals per client column - this can be used for sample weighting
        client_count = referral_table.groupby('Referral_ClientId')[['Referral_ReferralTakenDate']].count()
        client_count = client_count.rename(columns={'Referral_ReferralTakenDate':'TimeFeature_TotalReferralsForClient'})
        referral_table = referral_table.merge(client_count, left_on='Referral_ClientId', right_index=True)
        return referral_table

class ConsolidateTablesTransformer(BaseTransformer):
    """This transformer takes the dictionary of tables and produces a master referral table"""
    REQUIRED_TABLES = [
                        'Referral', 'ReferralDomesticCircumstances',
                       'ReferralIssue', 'Client', 'ReferralDietaryRequirements',
                       'ReferralBenefit', 'ReferralReason', 'ReferralDocument',
                       'ClientIssue'
    ]
    
    FLATTEN_TABLES_COLUMN_MAPPING = {
    'ReferralIssue': ['ReferralInstanceID', 'ClientIssueID'],
    'ReferralBenefit': ('ReferralInstanceId', 'BenefitTypeId'),
    'ReferralReason': ('ReferralInstanceID', 'ReferralReasonID'),
    'ReferralDietaryRequirements': ('ReferralInstanceID', 'DietaryRequirementsID'),
    'ReferralDomesticCircumstances': ('ReferralInstanceID', 'DomesticCircumstancesID'),
    'ReferralDocument': ('ReferralInstanceId','ReferralDocumentId'),
    }

    def transform(self, tables):
        rt = self.generate_master_referral_table(tables)        
        return rt
        
        
    def process_referral_table(self, referral_table):
        referral_table['ReferralTakenDate'] = pd.to_datetime(referral_table['ReferralTakenDate'])
        referral_table = referral_table.set_index('ReferralInstanceId')
        referral_table = referral_table.add_prefix('Referral_')
        return referral_table
    
    def process_client_table(self, client_table):
        client_table['ClientDateOfBirth'] = pd.to_datetime(client_table['ClientDateOfBirth'])
        client_table['AddressSinceDate'] = pd.to_datetime(client_table['AddressSinceDate'])
        client_table['Age'] = datetime.now() - client_table['ClientDateOfBirth']
        client_table['Age'] = client_table['Age'].dt.days / 365
        client_table.loc[client_table['Age'] < 0, 'Age'] += 100
        client_table['ClientIsMale'] *= 1
        client_table['KnownPartner'] = client_table['PartnerId'].notnull() * 1
        client_table['AddressLength'] = (datetime.now() - client_table['AddressSinceDate']).dt.days / 365

        dummied_cols = ['ClientEthnicityID', 'ClientCountryID', 'ClientAddressTypeID', 
                        'AddressPostCode', 'AddressLocalityId', 'ClientResidencyId']
        categories = pd.get_dummies(client_table[dummied_cols].astype(str),
                                   prefix=dummied_cols,
                                   prefix_sep='_')
        variables = ['Age', 'AddressLength', 'ClientIsMale', 'KnownPartner', 'ClientId']
        client_table = pd.concat([client_table[variables], categories], axis=1)
        client_table = client_table.add_prefix('Client_')
        return client_table
    
    def generate_master_referral_table(self, tables):
        # Check all the correct table entries are there
        for t in self.REQUIRED_TABLES:
            try:
                tables[t]
            except KeyError:
                 raise Exception("""{} table entry not found, this table is required,
                                     if there is no data then an empty dataframe 
                                     should be entered in the tables dictionary""".format(t))
                                 
        # Get the referral table and process
        if not tables['Referral'].empty:
            referral_table = tables['Referral']
            referral_table = self.process_referral_table(referral_table)
        else:
            raise Exception('Referral table contains no data, this table must be populated')
                                 
        # Flatten all other referral related tables and join to referral table
        flat_tables = {}
        for key in self.FLATTEN_TABLES_COLUMN_MAPPING.keys():
            if not tables[key].empty:
                flat_table = (tables[key].groupby(self.FLATTEN_TABLES_COLUMN_MAPPING[key])
                                                .size().unstack().add_prefix(key + '_'))
                referral_table = referral_table.merge(flat_table, left_index=True,
                                                      right_index=True, how='left')

        # Get the Client Table and process
        if not tables['Client'].empty:
            client_table = tables['Client']
            client_table = self.process_client_table(client_table)                             
        else:
            raise Exception('Client table contains no data, this table must be populated')
                                 
        # Get the Client Issue table and add to Client table
        if not tables['ClientIssue'].empty:
            client_issue_table = tables['ClientIssue'].groupby(['ClientId', 'ClientIssueId']).size().unstack()
            client_issue_table = client_issue_table.add_prefix('ClientIssue_')
            client_table = pd.concat([client_table, client_issue_table], axis=1)
        else:
            pass
        
        # Join Client and Referral Table together into master table
        master_table = referral_table.merge(client_table, left_on='Referral_ClientId',
                                            right_on='Client_ClientId', how='left')
        return master_table

class AddFutureReferralTargetFeatures(BaseTransformer):
    def __init__(self, window=365, break_length=28, break_coefficients=1):
        self.window = window
        self.break_length = break_length
        self.break_coefficients = break_coefficients


    def transform(self, referral_table):
        referral_table = self.calc_look_ahead_stats(referral_table, self.window,
                                                    self.break_length, self.break_coefficients)
        return referral_table
        
    def calc_look_ahead_stats(self, referrals, window=365, break_length=28, break_coefficient=1):
        all_ratios = []
        referral_no = referrals.assign(count=1).groupby('Referral_ClientId').expanding()['count'].sum()
        referral_no = referral_no.reset_index().set_index('level_1')['count']
        for i in tqdm(range(1, int(referral_no.max()))):
            # Grab the segment for each no of referrals
            segment = referrals.loc[referral_no == i, :]
            reference_date = segment.set_index('Referral_ClientId')['Referral_ReferralTakenDate']
            referrals = referrals.assign(reference_date=reference_date.reindex(referrals['Referral_ClientId']).values)
            date_diff = (referrals['Referral_ReferralTakenDate'] - referrals['reference_date']).dt.days
            year_range = referrals[(date_diff >= 0) & (date_diff <= window)]

            gaps = ((year_range.sort_values('Referral_ReferralTakenDate')
                     .groupby('Referral_ClientId')['Referral_ReferralTakenDate']
                     .diff().dt.days > break_length)
                    .groupby(year_range['Referral_ClientId']).sum())
            counts = (year_range.groupby('Referral_ClientId').size()) - 1
            future_referral_score = (counts - gaps * break_coefficient) / (window / 7)
            segment_ratios = pd.concat([counts, future_referral_score, gaps],
                                       axis=1).loc[segment['Referral_ClientId']]
            segment_ratios.columns = ['FutureReferralTargetFeature_FutureReferralCount',
                                        'FutureReferralTargetFeature_FutureReferralScore',
                                        'FutureReferralTargetFeature_FutureReferralGaps']
            segment_ratios.index = segment.index
            all_ratios.append(segment_ratios)
        all_ratios_df = pd.concat(all_ratios).reindex(referrals.index)
        referrals[all_ratios_df.columns] = all_ratios_df
        return referrals 

class SplitCurrentAndEverTransformer(BaseTransformer):
    """This Transformer takes the full referral dataframe and for a selected set of
        features splits them out into current referral features and ever referral features.
        e.g. current referral feature might be if a client has been referred by Agency 1
        but ever referral feature might be if a client has ever been referred by Agency1"""
    
    def __init__(self, features_classes_to_split):
        self.features_classes_to_split = features_classes_to_split

    def transform(self, referral_table):
        # Get list of features to split that are in dataframe
        features_to_split = []
        for c in self.features_classes_to_split:
            features_to_split += list(referral_table.filter(like=c).columns)

        # Get dataframe of current features
        current_features = referral_table[features_to_split]
        # Get any time features
        any_features = current_features.groupby(referral_table['Referral_ClientId'],
                                                as_index=False, sort=False).expanding().sum() > 0
        any_features.index = any_features.index.droplevel(0)
        # Re-index to the referral table
        any_features = any_features.loc[referral_table.index]
        # Remove the original features from referral table
        referral_table = referral_table.drop(features_to_split, axis=1)
        # Merge all three together
        return pd.concat([referral_table, current_features.add_suffix('_Current'),
                          any_features.add_suffix('_Ever')], axis=1)

class AlignFeaturesToColumnSchemaTransformer(object):
    """This transformer takes the column schema defined by the model and
        selects the features from the referral table using this schema
        any missing columns are filled with 0"""

    to_drop = ['Referral_StatusId', 'Referral_ReferralOnHold',
       'Referral_ReferralTakenDate', 'Referral_ReferralReadyDate',
       'Referral_ReferralCollectedDate', 'Referral_ReferralWorkerID',
       'Referral_ReferralPreparedWorkerId', 'Referral_ReferralHandedWorkerId',
       'Referral_ClientId', 'Referral_PartnerName', 'Referral_PartnerId',
       'Referral_DependantDetails', 'Referral_EthnicityId',
       'Referral_AddressLocalityId', 'Referral_AddressTypeId',
       'Referral_ReferralAgencyId', 'Referral_ReferralAgencyWorkerName',
       'Referral_ReferralAgencyTelephoneNumber', 'Referral_DietaryExtraNotes',
       'Referral_ReferralNotes', 'Referral_UpdateTimeStamp']

    to_drop += ['Client_ClientId', 'reference_date']

    to_drop += ['FutureReferralTargetFeature_FutureReferralCount',
                'FutureReferralTargetFeature_FutureReferralScore',
                'FutureReferralTargetFeature_FutureReferralGaps']

    to_drop += ['weeks']

    to_drop += ['TimeFeature_TotalReferralsForClient',
                'TimeFeature_BurstNumber', 'TimeFeature_ReferralNumber']


    def __init__(self):
        self.column_schema = None
        
    def fit_transform(self, referral_table):
        self.column_schema = list(referral_table.drop(self.to_drop, axis=1).columns)
        return self.transform(referral_table)

    def transform(self, referral_table):
        X = referral_table.reindex(self.column_schema, axis=1)
        y = referral_table['FutureReferralTargetFeature_FutureReferralScore']
        return X.fillna(0), y.fillna(0), referral_table[self.to_drop]

class FullTransformer(BaseTransformer):
    def __init__(self, features_to_split, column_schema):
        self.column_schema = column_schema
        self.features_to_split = features_to_split
    
    def fit_transform(self, tables_dict):
        return self.transform(tables_dict)


    def transform(self, tables_dict):
        consolidate = ConsolidateTablesTransformer()
        add_time_features = AddFutureReferralTargetFeatures()
        split_current_and_ever = SplitCurrentAndEverTransformer(self.features_to_split)
        align = AlignFeaturesToColumnSchemaTransformer(self.column_schema)
        referral_table = consolidate.fit_transform(tables_dict)
        referral_table = add_time_features.fit_transform(referral_table)
        referral_table = split_current_and_ever.fit_transform(referral_table)
        X, y, referral_table = align.fit_transform(referral_table)
        return X, y, referral_table

class TimeWindowFeatures(BaseTransformer):
    def __init__(self, windows):
        self.windows = windows

    def get_rolling_count(self, referrals, window_size=10):
        unrolled = referrals.set_index('Referral_ReferralTakenDate').groupby('Client_ClientId').apply(
            lambda k: k.groupby(pd.TimeGrouper('1W', convention='e')).size())

        referrals['weeks'] = referrals['Referral_ReferralTakenDate'] - pd.to_timedelta(
            referrals['Referral_ReferralTakenDate'].dt.dayofweek, unit='d') + pd.to_timedelta(6, unit='d')
        referrals['weeks'] = pd.to_datetime(referrals['weeks'].dt.date)

        weighted = unrolled.groupby('Client_ClientId').apply(lambda k: k.rolling(window=window_size, min_periods=1)
                                                             .sum().shift(1)).reset_index()

        return referrals.merge(weighted, right_on=['Client_ClientId', 'Referral_ReferralTakenDate'], left_on=['Client_ClientId', 'weeks']) \
            .set_index(referrals.index)[0].fillna(0)

    def get_all_rolling_counts(self, windows, referrals):
        df = pd.DataFrame(index=referrals.index)
        for i in windows:
            ewm = self.get_rolling_count(referrals, i)
            df['window_count_{}'.format(i)] = ewm
        return df

    def fit_transform(self, X):
        time_features = self.get_all_rolling_counts(self.windows, X)
        return pd.concat([X, time_features], axis=1)


class TransformerPipeline(BaseTransformer):
    def __init__(self, steps, aligner):
        self.pipeline = steps
        self.aligner = aligner

    def transform(self, X):
        for transformer in self.pipeline:
            X = transformer.fit_transform(X)

        return self.aligner.fit_transform(X)

class ParseJSONToTablesTransformer(BaseTransformer):
    """This transformer takes the json from the request
    and turns it into a dictionary of tables"""

    def transform(self, request_json_string):
        json_data = json.loads(request_json_string)
        tables_dict = {}
        for k, v in json_data.items():
            if v:
                tables_dict[k] = pd.DataFrame(v)
            else:
                tables_dict[k] = pd.DataFrame()
        return tables_dict

class TrainingDataGenerator(object):
    SQL_DICT = {'Referral': """SELECT * FROM Referral;""",
                
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
    
    def __init__(self, database_path):
        self.con = sqlite3.connect(database_path)

    def get_training_data(self, limit=None):
        tables_dict = {k: pd.read_sql(v, con=self.con) for k, v in self.SQL_DICT.items()}

        if limit is not None:
            tables_dict['Referral'] = (tables_dict['Referral'][tables_dict['Referral']
                                                ['ReferralInstanceId'] < limit])

        return tables_dict
