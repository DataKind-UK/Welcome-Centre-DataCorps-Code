# Welcome Centre DataKind DataCorps Project

### Background and context

The Welcome Centre is intended to provide short-term support to people experiencing a personal crisis, in the form of parcels containing food and other household items. If the crisis causing a need for parcels is not addressed, clients can develop a dependency on the support of the Welcome Centre. A support worker provides advice to those using the service to help address the underlying problems, and reduce the risk of such dependency developing.

Identifying those in need of support is challenging, and compounded by the fact that often the only opportunity to speak with clients is when they arrive to collect their parcels. As a result, identifying people who might need to speak with the support worker when they are referred to the organisation to receive a parcel is important. Currently such clients are identified by their having requested a certain number of packs in the last six months.

### Aims of the project

To develop an automated method for identifying those clients who should be referred to the support worker. 

This needs to take place when volunteers are speaking to them on the phone and entering information into the client database. 

[Lucy to expand]

### Overview of the model

* [Dependency issue - Lucy]

Lucy notes
  * Initial view = more referrals (referrals>x) determine dependency
  * But challenges: 1)Our observation history for each client is a different length 2) Trade-off between referral number and saturation
  * And have information on gaps between referals
  * Dependency Score = (Number of Referrals in next 12 months - Number of Gaps) / 52
  * Gap = period longer than 28 days - want to penalise sustained unbroken usage.


  

* [What the model predicts, dependency score, 1 year window etc. - ???]
* [High-level what the model is and why it is a sensible choice]

### Technical detail



### Guide to installation/setup

