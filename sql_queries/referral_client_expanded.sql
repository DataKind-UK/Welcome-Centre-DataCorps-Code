--referral and client expanded
select * from Referral
left join ReferralStatus on ReferralStatus.ReferralStatusId = Referral.StatusId
left join (
  select * from Client
left join Ethnicity on Client.ClientEthnicityID = Ethnicity.EthnicityID
left join Country on Client.ClientCountryID = Country.CountryId
left join ResidencyStatus on Client.ClientResidencyId = ResidencyStatus.ResidencyId
left join ClientAddressType on Client.ClientAddressTypeID = ClientAddressType.ClientAddressTypeID
left join Locality on Client.AddressLocalityId = Locality.LocalityId
  ) as client on client.ClientId = Referral.ClientId

left join Ethnicity on Referral.EthnicityId = Ethnicity.EthnicityID
left join Locality on Referral.AddressLocalityId = Locality.LocalityId
left join ClientAddressType on Referral.AddressTypeId = ClientAddressType.ClientAddressTypeID
left join ReferralAgency on Referral.ReferralAgencyId = ReferralAgency.ReferralAgencyId

where client.ClientId NOTNULL ;

--many to one tables
select * from ReferralBenefit
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
  inner join ReferralReasonCodes on ReferralReasonCodes.ReferralReasonId = ReferralReason.ReferralReasonID;
