# Welcome Centre DataKind DataCorps Project

## Background and aims

The Welcome Centre (TWC) is intended to provide short-term support to people experiencing a personal crisis, in the form of parcels containing food and other household items. If the crisis causing a need for parcels is not addressed, clients can develop a dependency on the support of the Welcome Centre. A support worker provides advice to those using the service to help address the underlying problems, and reduce the risk of such dependency developing.

Identifying those in need of support is challenging, and compounded by the fact that often the only opportunity to speak with clients is when they arrive to collect their parcels. As a result, identifying people who might need to speak with the support worker when they are referred to the organisation to receive a parcel is important. Currently such clients are identified by their having requested a certain number of packs in the last six months.

DataKind UK and TWC partnered to build an automated method for identifying those clients who should be referred to the support worker. This needs to take place when volunteers are speaking to them on the phone and entering information into the client database. 

## Overview of the model

### Definitions

* Client: a person who is referred by an agency (e.g., council support worker) to TWC
* Referral: Each time a person needs a package from TWC, a support agency has to call TWC to request the package. This is the referral. Once the referral is made & accepted, the client mustgo to TWC to collect the package

### What is dependency?
   
Our aim was to predict whether a client would become dependent on TWC help. To do this, we needed to define "dependent". There was not a clear definition of when someone was dependent, but there were views around dependency:
   1. More referrals is worse
   2. Longer gaps between referrals shows an indication of a client being able to "get back on their feet" and lower dependency
   3. There are some referral reasons (e.g., universal credit, asylum seeker) that mean a person would come back for a certain period of time, and this was to be expected and need not be predicted.
We agreed that (1) and (2) would be part of the predictive model, but (3) would be dealt with in "business logic" outside the model that could then be updated more easily by TWC (e.g., in case of changes to benefits).

Just to give some flavour as to why only considering (1) (the number of referrals) over any time period didn't capture the dynamics of client bahaviour. Consider 2 people with 6 referrals in our data: Person one has 6 referrals over the last 6 week period, but Person 2 their 6 referrals randomly across a 12 month period. Person 1 appears to have developed a pattern of dependence, but this simpler measure of referrals doesn't capture this.

Another approach we considered was a definition of dependency of X number of referrals in Y months. However, choosing the number of referrals and the time period was not straightforward. For example, consider 2 people with 6 referrals in 3 months: Person one has 6 referrals over a 6 week period and Person 2 has 2 referrals at the beginning of 3 months, then a 4 week break, then 4 more referrals. Should there be a difference in dependency score? This approach didn't allow for understanding the gaps between referrals over a long enough period.

Our approach was to take information on both the referral numbers and time between referrals over the past 12 months of a client. We created our dependency score as:

**Dependency Score = (Number of Referrals in next 12 months - Number of Gaps) / 52**

This results in a dependency score between 0 and 1, with 1 indicating referrals for all 52 weeks in the year.

"Gap" was defined as a period of 4 weeks without a referral. As discussions with TCW indicated that a gap of 4 weeks between referrals was enough to suggest a person had "got back on their feet" for a bit. 

This formula for dependency had the advantages that:
   * By looking over 12 months lessened the impact of the observation length on the score
   * Including the "gaps" term created a penalty term if there were spells of unbroken / dependent usage
   
One disadvantage of this definition of dependence is the complexity and interpretability as the score between 0 and 1 is not easily interpretable as a behaviour.
    
### Data available

There was data available for teh past 3 years of TWC activity. This data covered client’s history of referrals, client issues, referral issues and personal characteristics. Tis data was used to create features in 4 areas:
1. Client characteristics
2. Referral history pattern
3. Hitorical referral characteristics
4. Current referral

![Overview of data features](https://github.com/DataKind-UK/Welcome-Centre-DataCorps-Code/blob/lucydocbranch/summary%20of%20data.PNG)

### Modeling Approach

Every time a client is referred, the model will predict the dependency score based on the client’s history of referrals, client issues, referral issues and personal characteristics.

The statistical model that was used to predict dependency is a predictive algorithm known as a random forest. This is a common method used by data scientists working with large structured datasets. The advantages of this algorithm are high predictive power and the ability to find non-linear relationships between our target variable (dependency) and explanatory variables (e.g. referral issue or time since last referral). The disadvantages of this model are that it can be too specific to information that it has seen before (and not be a good predictor of new behaviour), and the ability to understand the nature of the relationships between variables in the model is limited.

The team chose this algorithm as it provided the best performance on a test set. Linear Ridge regression models were also tested as an approach to understanding the relationship, however these models did not perform as well.

### Interpreting the model results

The model produces a predicted dependency score between 0 and 100. This score does not by itself show the whole picture as to whether a client is more likely to become dependent. What can provide more insight into a client’s behaviour is whether their current trend of referrals make it more likely that they are becoming dependent.

To understand a client’s trend, we can use both the historical score and the predicted score. Every time a client is referred, the historical dependency score is calculated. For example, if this is the client’s first referral they would get a historical score of (1/52)*100 = 1.9. This historical dependency score is combined with the predicted dependency score to give a ratio:

**Score Ratio = Predicted Score / Historical Score**

This effectively measures how we expect a client’s usage of TWC to increase or decrease over time. A Score Ratio of 2.0 means we expect the client to come back twice as many times as they came before. This ratio is designed to avoid the system only referring heavy users to the support worker (TWC would likely have already seen them many times before) but instead identify users whose needs might be increasing, and who therefore may benefit from support.

If the Score Ratio is over the set threshold, the client is flagged for referral to the support worker. The threshold below has been calibrated to achieve a 10% support worker referral. This can be easily adjusted over time. 
