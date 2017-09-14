import pandas as pd
import sqlite3

path = '../../Welcome-Centre-DataCorps-Data/ClientDatabaseStructure.mdb.sqlite'
con = sqlite3.connect(path)

referrals = pd.read_sql("""select * from Referral
left join ReferralStatus on ReferralStatus.ReferralStatusId = Referral.StatusId
left join Ethnicity on Referral.EthnicityId = Ethnicity.EthnicityID
left join Locality on Referral.AddressLocalityId = Locality.LocalityId
left join ClientAddressType on Referral.AddressTypeId = ClientAddressType.ClientAddressTypeID
left join ReferralAgency on Referral.ReferralAgencyId = ReferralAgency.ReferralAgencyId
left join (select count(*) as pack_count, ReferralInstanceID from ReferralPack GROUP BY ReferralInstanceID) as pack_count
on pack_count.ReferralInstanceID = Referral.ReferralInstanceId;""", con)
referrals = referrals.set_index('ReferralInstanceId')

clients = pd.read_sql("""select * from Client
left join Ethnicity on Client.ClientEthnicityID = Ethnicity.EthnicityID
left join Country on Client.ClientCountryID = Country.CountryId
left join ResidencyStatus on Client.ClientResidencyId = ResidencyStatus.ResidencyId
left join ClientAddressType on Client.ClientAddressTypeID = ClientAddressType.ClientAddressTypeID
left join Locality on Client.AddressLocalityId = Locality.LocalityId""", con)
clients = clients.set_index('ClientId')

table_sql = """select * from ReferralBenefit
  inner join BenefitType on BenefitType.BenefitTypeId = ReferralBenefit.BenefitTypeId;
select * from ReferralDietaryRequirements
  inner join DietaryRequirements on ReferralDietaryRequirements.DietaryRequirementsID = DietaryRequirements.DietaryRequirementsID;
select * from ReferralDocument
  inner join DocumentEvidence on ReferralDocument.ReferralDocumentId = DocumentEvidence.DocumentEvidenceId;
select * from ReferralDomesticCircumstances
  inner join DomesticCircumstances on DomesticCircumstances.DomesticCircumstancesID = ReferralDomesticCircumstances.DomesticCircumstancesID;
select * from ReferralIssue
  inner join ClientIssueCodes on ClientIssueCodes.ClientIssueId = ReferralIssue.ClientIssueID;
select * from ReferralReason
  inner join ReferralReasonCodes on ReferralReasonCodes.ReferralReasonId = ReferralReason.ReferralReasonID;"""
table_names = [t.split(' ')[3].strip() for t in table_sql.split(';')[:-1]]
tables = {table_names[i]: pd.read_sql(t.strip(), con) for i, t in enumerate(table_sql.split(';')[:-1])}

packs = pd.read_sql("""select * from ReferralPack""", con)
pack_descriptions = pd.read_sql("""select * from PackList""", con)



column_mapping = {
    'ReferralIssue': 'ClientIssueDescription',
    'ReferralBenefit': 'BenefitTypeName',
    'ReferralReason': 'ReferralReasonDescription',
    'ReferralDietaryRequirements': 'DietaryRequirementsDescription',
    'ReferralDomesticCircumstances': 'DomesticCircumstancesDescription',
    'ReferralDocument': 'DocumentEvidenceDescription'
}

flat_tables = {t: tables[t].groupby([tables[t].iloc[:,0], column_mapping[t]]).size().unstack().add_prefix(t + '_') for t in tables}

for t in flat_tables:
    referrals = referrals.merge(flat_tables[t], left_index=True, right_index=True, how='left')

referrals.to_csv('../../Welcome-Centre-DataCorps-Data/referrals.csv')
clients.to_csv('../../Welcome-Centre-DataCorps-Data/clients.csv')
packs.to_csv('../../Welcome-Centre-DataCorps-Data/packs.csv')
pack_descriptions.to_csv('../../Welcome-Centre-DataCorps-Data/pack_descriptions.csv')