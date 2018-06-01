-- This file contains all the queries required to generate the data in ./requst.json
-- As an example we have used the below reference data:
-- --> ClientId = 10
-- --> ReferralInstanceId being scored = 15739


-- Referral Table
-- Return all referrals up to the ReferralInstanceID being scored
SELECT * FROM Referral where ClientId = 10 and ReferralInstanceId <= 15739;

-- Client Table
-- Return the Client Row for the Client in question
SELECT * FROM Client where ClientId = 10;

-- Referral Benefit
--- Return all Referral Benefits for all referrals up to the ReferralInstanceID being scored
SELECT ref_dim.* FROM ReferralBenefit as ref_dim
LEFT JOIN  Referral on Referral.ReferralInstanceId = ref_dim.ReferralInstanceId
WHERE Referral.ClientId = 10 and Referral.ReferralInstanceId <= 15739;

-- Referral Dietary Requirements
--- Return all Referral Dietary Requirements for all referrals up to 
--- the ReferralInstanceID being scored
SELECT ref_dim.* FROM ReferralDietaryRequirements as ref_dim
LEFT JOIN  Referral on Referral.ReferralInstanceId = ref_dim.ReferralInstanceId
WHERE Referral.ClientId = 10 and Referral.ReferralInstanceId <= 15739;

-- Referral Document 
--- Return all Referral Documents for all referrals up to the ReferralInstanceID being scored
SELECT ref_dim.* FROM ReferralDocument as ref_dim
LEFT JOIN  Referral on Referral.ReferralInstanceId = ref_dim.ReferralInstanceId
WHERE Referral.ClientId = 10 and Referral.ReferralInstanceId <= 15739;

-- ReferralDomesticCircumstances
--- Return all ReferralDomesticCircumstances for all referrals up to the ReferralInstanceID being scored
SELECT ref_dim.* FROM ReferralDomesticCircumstances as ref_dim
LEFT JOIN  Referral on Referral.ReferralInstanceId = ref_dim.ReferralInstanceId
WHERE Referral.ClientId = 10 and Referral.ReferralInstanceId <= 15739;

-- ReferralIssue
--- Return all ReferralIssue for all referrals up to the ReferralInstanceID being scored
SELECT ref_dim.* FROM ReferralIssue as ref_dim
LEFT JOIN  Referral on Referral.ReferralInstanceId = ref_dim.ReferralInstanceId
WHERE Referral.ClientId = 10 and Referral.ReferralInstanceId <= 15739;

-- ReferralReason
--- Return all ReferralReason for all referrals up to the ReferralInstanceID being scored
SELECT ref_dim.* FROM ReferralReason as ref_dim
LEFT JOIN  Referral on Referral.ReferralInstanceId = ref_dim.ReferralInstanceId
WHERE Referral.ClientId = 10 and Referral.ReferralInstanceId <= 15739;

-- Client Issue
-- Return all ClientIssues for the Client
SELECT * FROM ClientIssue where ClientId = 10;







