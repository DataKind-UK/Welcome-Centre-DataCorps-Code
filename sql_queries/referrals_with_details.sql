select
  r.ReferralCollectedDate,
  r.ReferralReadyDate,
  r.ReferralTakenDate,
  c.ClientDateOfBirth,
  c.ClientId,
  ra.ReferralAgencyName,
  rs.ReferralStatusDescription
from Referral as r
LEFT JOIN ReferralStatus as rs
on rs.ReferralStatusId = r.StatusId
left join Client as c
on c.ClientId = r.ClientId
left join ReferralAgency as ra
on ra.ReferralAgencyID = r.ReferralAgencyId


