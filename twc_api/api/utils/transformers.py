import pandas as pd
# from tqdm import tqdm
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
        self.dataset_start_date = referral_table['referral_referraltakendate'].min()
        return self.transform(referral_table)

    def transform(self, referral_table):
        valid_referrals = (referral_table[referral_table['referral_referraltakendate']
                            >= self.dataset_start_date].copy())
        valid_referrals['timefeature_referralnumber'] =( valid_referrals
                                                        .groupby('referral_clientid')
                                                        ['referral_referraltakendate'].rank())
        referral_table['timefeature_referralnumber'] = (valid_referrals['timefeature_referralnumber']
                                                        .loc[referral_table.index])
        # Add burst features
        referral_table = referral_table.sort_values('referral_referraltakendate')
        referral_table['timefeature_dayssincelastreferral'] = (referral_table.groupby('referral_clientid')
                                                    ['referral_referraltakendate'].diff().dt.days
                                                    .fillna(0))
        referral_table['timefeature_startofburst'] = (referral_table['timefeature_dayssincelastreferral']
                                                      > self.break_length)
        referral_table['timefeature_burstnumber'] = (referral_table.groupby('referral_clientid')
                                                    ['timefeature_startofburst'].cumsum() + 1)
        referral_table['timefeature_indexinburst'] = (referral_table.groupby(
                                    ['referral_clientid', 'timefeature_burstnumber'])
                                        ['referral_referraltakendate'].rank())
        ## Add a total referrals per client column - this can be used for sample weighting
        client_count = referral_table.groupby('referral_clientid')[['referral_referraltakendate']].count()
        client_count = client_count.rename(columns={'referral_referraltakendate':'timefeature_totalreferralsforclient'})
        referral_table = referral_table.merge(client_count, left_on='referral_clientid', right_index=True)
        return referral_table

class ConsolidateTablesTransformer(BaseTransformer):
    """This transformer takes the dictionary of tables and produces a master referral table"""
    REQUIRED_TABLES = [
                        'referral', 'referraldomesticcircumstances',
                       'referralissue', 'client', 'referraldietaryrequirements',
                       'referralbenefit', 'referralreason', 'referraldocument',
                       'clientissue'
    ]
    
    FLATTEN_TABLES_COLUMN_MAPPING = {
    'referralissue': ['referralinstanceid', 'clientissueid'],
    'referralbenefit': ('referralinstanceid', 'benefittypeid'),
    'referralreason': ('referralinstanceid', 'referralreasonid'),
    'referraldietaryrequirements': ('referralinstanceid', 'dietaryrequirementsid'),
    'referraldomesticcircumstances': ('referralinstanceid', 'domesticcircumstancesid'),
    'referraldocument': ('referralinstanceid','referraldocumentid'),
    }

    def __init__(self, count_encode):
        self.count_encode = count_encode

    def transform(self, tables):
        rt = self.generate_master_referral_table(tables, training=False)
        return rt

    def fit_transform(self, tables):
        rt = self.generate_master_referral_table(tables, training=True)
        return rt
        
    def process_referral_table(self, referral_table):
        referral_table['referraltakendate'] = pd.to_datetime(referral_table['referraltakendate'])
        referral_table = referral_table.add_prefix('referral_')
        return referral_table
    
    def process_client_table(self, client_table, training=True):
        client_table['clientdateofbirth'] = pd.to_datetime(client_table['clientdateofbirth'])
        client_table['addresssincedate'] = pd.to_datetime(client_table['addresssincedate'])
        client_table['age'] = datetime.now() - client_table['clientdateofbirth']
        client_table['age'] = client_table['age'].dt.days / 365
        client_table.loc[client_table['age'] < 0, 'age'] += 100
        client_table['clientismale'] *= 1
        if 'partnerid' in client_table.columns:
            client_table['knownpartner'] = client_table['partnerid'].notnull() * 1
        else:
            client_table['knownpartner'] = 0
        client_table['addresslength'] = (datetime.now() -
                                             client_table['addresssincedate']).dt.days / 365

        dummied_cols = ['clientcountryid', 'clientaddresstypeid', 
                        'addresspostcode', 'addresslocalityid', 'clientresidencyid']
        client_table[dummied_cols] = client_table[dummied_cols].astype(str)
        if self.count_encode:
            if training:
                self.replacements = {col: client_table[col].replace(client_table[col].value_counts()) for col in dummied_cols}

            categories = pd.get_dummies(client_table.assign(**self.replacements)[dummied_cols],
                                        columns=dummied_cols,
                                        prefix_sep='_')
        else:
            categories = pd.get_dummies(client_table[dummied_cols],
                                       columns=dummied_cols,
                                       prefix_sep='_')
        variables = ['age', 'addresslength', 'clientismale', 'knownpartner', 'clientid']
        client_table = pd.concat([client_table[variables], categories], axis=1)
        client_table = client_table.add_prefix('client_')
        return client_table
    
    def generate_master_referral_table(self, tables, training=True):
        # Check all the correct table entries are there
        for t in self.REQUIRED_TABLES:
            try:
                tables[t]
            except KeyError:
                 raise Exception("""{} table entry not found, this table is required,
                                     if there is no data then an empty dataframe 
                                     should be entered in the tables dictionary""".format(t))
                                 
        # Get the referral table and process
        if not tables['referral'].empty:
            referral_table = tables['referral']
            referral_table = self.process_referral_table(referral_table)
        else:
            raise Exception('referral table contains no data, this table must be populated')
                                 
        # Flatten all other referral related tables and join to referral table
        flat_tables = {}
        for key in self.FLATTEN_TABLES_COLUMN_MAPPING.keys():
            if not tables[key].empty:
                if self.count_encode:
                    item_id = self.FLATTEN_TABLES_COLUMN_MAPPING[key][1]
                    if training:
                        self.encoding = tables[key][item_id].replace(tables[key].groupby(item_id).size())

                    flat_table = (tables[key].assign(**{item_id: self.encoding}).groupby(self.FLATTEN_TABLES_COLUMN_MAPPING[key])
                                                .size().unstack().add_prefix(key + '_'))
                else:
                    flat_table = (tables[key].groupby(self.FLATTEN_TABLES_COLUMN_MAPPING[key])
                                                .size().unstack().add_prefix(key + '_'))
                referral_table = referral_table.merge(flat_table, left_on='referral_referralinstanceid',
                                                      right_index=True, how='left')

        # Get the Client Table and process
        if not tables['client'].empty:
            client_table = tables['client']
            client_table = self.process_client_table(client_table)                             
        else:
            raise Exception('Client table contains no data, this table must be populated')
                                 
        # Get the Client Issue table and add to Client table
        if not tables['clientissue'].empty:
            client_issue_table = tables['clientissue'].groupby(['clientid', 'clientissueid']).size().unstack()
            client_issue_table = client_issue_table.add_prefix('clientissue_')
            client_table = pd.merge(client_table, client_issue_table,
                                    left_on='client_clientid', right_index=True, how='left')
        else:
            pass
        
        # Join Client and referral Table together into master table
        master_table = referral_table.merge(client_table, left_on='referral_clientid',
                                            right_on='client_clientid', how='left')

        # Order by referral taken date - this is important for spliting the train/test sets
        master_table = master_table.sort_values('referral_referraltakendate')
        return master_table.set_index('referral_referralinstanceid')

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
        referral_no = referrals.assign(count=1).groupby('referral_clientid').expanding()['count'].sum()
        referral_no = referral_no.reset_index().set_index('referral_referralinstanceid')['count']
        for i in range(1, int(referral_no.max())):
            # Grab the segment for each no of referrals
            segment = referrals.loc[referral_no == i, :]
            reference_date = segment.set_index('referral_clientid')['referral_referraltakendate']
            referrals = referrals.assign(reference_date=reference_date.reindex(referrals['referral_clientid']).values)
            date_diff = (referrals['referral_referraltakendate'] - referrals['reference_date']).dt.days
            year_range = referrals[(date_diff >= 0) & (date_diff <= window)]

            gaps = ((year_range.sort_values('referral_referraltakendate')
                     .groupby('referral_clientid')['referral_referraltakendate']
                     .diff().dt.days > break_length)
                    .groupby(year_range['referral_clientid']).sum())
            counts = (year_range.groupby('referral_clientid').size()) - 1
            future_referral_score = (counts - gaps * break_coefficient) / (window / 7)
            segment_ratios = pd.concat([counts, future_referral_score, gaps],
                                       axis=1).loc[segment['referral_clientid']]
            segment_ratios.columns = ['futurereferraltargetfeature_futurereferralcount',
                                        'futurereferraltargetfeature_futurereferralscore',
                                        'futurereferraltargetfeature_futurereferralgaps']
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
        any_features = current_features.groupby(referral_table['referral_clientid'],
                                                as_index=False, sort=False).expanding().sum() > 0
        any_features.index = any_features.index.droplevel(0)
        # Re-index to the referral table
        any_features = any_features.loc[referral_table.index]
        # Remove the original features from referral table
        referral_table = referral_table.drop(features_to_split, axis=1)
        # Merge all three together
        return pd.concat([referral_table, current_features.add_suffix('_current'),
                          any_features.add_suffix('_ever')], axis=1)

class AlignFeaturesToColumnSchemaTransformer(object):
    """This transformer takes the column schema defined by the model and
        selects the features from the referral table using this schema
        any missing columns are filled with 0"""

    to_drop = ['referral_statusid', 'referral_referralonhold',
       'referral_referraltakendate', 'referral_referralreadydate',
       'referral_referralcollecteddate', 'referral_referralworkerid',
       'referral_referralpreparedWorkerid', 'referral_referralhandedworkerid',
       'referral_clientid', 'referral_partnername', 'referral_partnerid',
       'referral_dependantdetails', 'referral_ethnicityid',
       'referral_addresslocalityid', 'referral_addresstypeid',
       'referral_referralagencyid', 'referral_referralagencyworkername',
       'referral_referralagencytelephonenumber', 'referral_dietaryextranotes',
       'referral_referralnotes', 'referral_updatetimestamp']

    to_drop += ['client_clientid', 'reference_date']

    to_drop += ['futurereferraltargetfeature_futurereferralcount',
                'futurereferraltargetfeature_futurereferralscore',
                'futurereferraltargetfeature_futurereferralgaps']

    to_drop += ['weeks']

    to_drop += ['timefeature_totalreferralsforclient',
                'timefeature_burstnumber', 'timefeature_referralnumber']


    def __init__(self):
        self.column_schema = None
        
    def fit_transform(self, referral_table):
        self.column_schema = list(referral_table.drop(self.to_drop, axis=1, errors='ignore').columns)
        return self.transform(referral_table)

    def transform(self, referral_table):
        referral_table = referral_table.sort_values('referral_referraltakendate')
        X = referral_table.reindex(self.column_schema, axis=1)
        y = referral_table['futurereferraltargetfeature_futurereferralscore']
        return X.fillna(0), y.fillna(0), referral_table.drop(X.columns, axis=1, errors='ignore')

class FullTransformer(BaseTransformer):
    def __init__(self, features_to_split, column_schema):
        self.column_schema = column_schema
        self.features_to_split = features_to_split
    
    def fit_transform(self, tables_dict):
        return self.transform(tables_dict)


    def transform(self, tables_dict):
        consolidate = ConsolidateTablesTransformer()
        add_time_features = AddFuturereferralTargetFeatures()
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
        unrolled = referrals.set_index('referral_referraltakendate').groupby('Client_Clientid').apply(
            lambda k: k.groupby(pd.TimeGrouper('1W', convention='e')).size())
        # if only one client, split-apply-combine returns strange shape
        if unrolled.index.name == 'Client_Clientid':
            unrolled = unrolled.T
            unrolled['Client_Clientid'] = unrolled.columns[0]
            unrolled.columns = [0, 'Client_Clientid']
        referrals['weeks'] = referrals['referral_referraltakendate'] - pd.to_timedelta(
            referrals['referral_referraltakendate'].dt.dayofweek, unit='d') + pd.to_timedelta(6, unit='d')
        referrals['weeks'] = pd.to_datetime(referrals['weeks'].dt.date)

        weighted = unrolled.groupby('Client_Clientid').apply(lambda k: k.rolling(window=window_size, min_periods=1)
                                                             .sum()).reset_index()

        merged = referrals.merge(weighted, right_on=['Client_Clientid', 'referral_referraltakendate'],
                                 left_on=['Client_Clientid', 'weeks'])[0].fillna(0)
        return merged

    def get_all_rolling_counts(self, windows, referrals):
        df = pd.DataFrame(index=referrals.index)
        for i in windows:
            ewm = self.get_rolling_count(referrals, i)
            df['window_count_{}'.format(i)] = ewm
        return df

    def fit_transform(self, X):
        time_features = self.get_all_rolling_counts(self.windows, X)
        return pd.concat([X, time_features], axis=1)

    def transform(self, X):
        return self.fit_transform(X)

class TransformerPipeline(BaseTransformer):
    def __init__(self, steps, aligner):
        self.pipeline = steps
        self.aligner = aligner

    def fit_transform(self, X):
        for transformer in self.pipeline:
            X = transformer.fit_transform(X)

        return self.aligner.fit_transform(X)

    def transform(self, X):
        for transformer in self.pipeline:
            X = transformer.transform(X)

        return self.aligner.transform(X)

class ParseJSONToTablesTransformer(BaseTransformer):
    """This transformer takes the json from the request
    and turns it into a dictionary of tables"""

    def transform(self, req_json):
        if type(req_json) == str:
            json_data = json.loads(req_json)
        else:
            json_data = req_json
        tables_dict = {}
        for k, v in json_data.items():
            if v:
                tables_dict[k] = pd.DataFrame(v)
            else:
                tables_dict[k] = pd.DataFrame()
        return tables_dict

class TrainingDataGenerator(object):
    SQL_DICT = {'referral': """SELECT * FROM referral;""",
                
                'client': """SELECT * FROM Client;""",
                
                'referralbenefit':"""SELECT ref_dim.* FROM referralbenefit as ref_dim
                LEFT JOIN  referral on referral.referralinstanceid = ref_dim.referralinstanceid;""",
                
                'referraldietaryrequirements':"""SELECT ref_dim.* FROM referraldietaryRequirements as ref_dim
                LEFT JOIN  referral on referral.referralinstanceid = ref_dim.referralinstanceid;""",

                'referraldocument':"""SELECT ref_dim.* FROM referraldocument as ref_dim
                LEFT JOIN  referral on referral.referralinstanceid = ref_dim.referralinstanceid;""",

                'referraldomesticcircumstances': """SELECT ref_dim.* FROM referralDomesticCircumstances as ref_dim
                LEFT JOIN  referral on referral.referralinstanceid = ref_dim.referralinstanceid;""",

                'referralissue':"""SELECT ref_dim.* FROM referralIssue as ref_dim
                LEFT JOIN  referral on referral.referralinstanceid = ref_dim.referralinstanceid;""",

                'referralreason': """SELECT ref_dim.* FROM referralReason as ref_dim
                LEFT JOIN  referral on referral.referralinstanceid = ref_dim.referralinstanceid;""",

                'clientissue': """SELECT * FROM clientissue;"""
                }
    
    def __init__(self, database_path):
        self.con = sqlite3.connect(database_path)

    def get_training_data(self, limit=None):
        tables_dict = {k: pd.read_sql(v, con=self.con) for k, v in self.SQL_DICT.items()}
        for k, t in tables_dict.items():
            t.columns = [c.lower() for c in t.columns]

        if limit is not None:
            tables_dict['referral'] = (tables_dict['referral'][tables_dict['referral']
                                                ['referralinstanceid'] < limit])

        return tables_dict
