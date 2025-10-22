# u_cog_minister_updates.py
### Purpose: Retrieve minister update surveys from the Cognito API and sync to the minister_surveys table in fmcusa_gl

## Source
Online form on Cognito website: https://www.cognitoforms.com/FMCUSA1/MinisterSurvey

## Frequency
As needed, but particularly before reminder emails for Minister Surveys are run.

## Upstream Requirements
Must add any missing minister ids prior to running. Data obtained directly from ministers - no fmc data used as input.

## Downstream Dependencies
Most minister data relies on this survey. Yearbook data as reported in Domo and in publications, demographic information when cross-referenced with appointment data, and other uses as requested on an ad-hoc basis.
