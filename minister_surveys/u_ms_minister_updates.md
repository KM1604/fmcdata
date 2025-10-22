# u_ms_minister_updates.py
### Purpose: Retrieve minister update surveys from the Cognito API and sync to the minister_surveys table in fmcusa_gl

## Source
Online MS form: https://forms.office.com/Pages/ResponsePage.aspx?id=3AlnUp5kBU-SUhKseoauoR9nnOCKpEBMmdU_N7OMNpxUMFdZQTI1MlVHU1JJUzdTM1FVNFI1TUpHUS4u 
Also includes two other forms: one in Spanish and one in Swahili.
Power Automate Flow: populates Excel file used as input to this script based on responses to the three MS forms.

## Frequency
As needed, but phased out in Nov 2025. No new submissions should be incoming.

## Upstream Requirements
Power Automate Flow updates file in SharePoint. Sync to local drive must be complete before script runs.

## Downstream Dependencies
Most minister data relies on this survey. Yearbook data as reported in Domo and in publications, demographic information when cross-referenced with appointment data, and other uses as requested on an ad-hoc basis. This survey has been depreciated as of November 2025 and is no longer an active form.
