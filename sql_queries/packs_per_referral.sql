select Referral.ReferralInstanceId, count(*) as pack_count from Referral
left join ReferralPack
on ReferralPack.ReferralInstanceID = Referral.ReferralInstanceId
GROUP BY Referral.ReferralInstanceId
