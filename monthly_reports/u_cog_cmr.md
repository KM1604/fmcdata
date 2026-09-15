# Update Pipeline for importing Church Monthy Reports in Cognito to fmcusa_gl
### Purpose: receive and retain the monthly church reports as submitted by local congregations

## Source
* Cognito Forms website has entries saved on their site. Local .csv copy of entries is saved to the local folder.
* Form ID, API key, and API key expiration date saved in cfg.toml file

## Frequency
* runs nightly
* updates to cmr should be edited on Cognito and this pipeline run to update SQL db on fmcusa gl

## Upstream Requirements
* API app maintained on Cognito
* edits made to Cognito
* cfg.toml file contains necessary schema changes to Cognito to match fmcusa
* Stored procedure on fmcusa gl contains logic needed to merge formatted source table into live prod table

## Downstream Dependencies
* Power Automate Script reads from SharePoint xlsx file with embedded Power Query to apply logic to determine which ministers need a reminder
* Power Automate Script to send minister update requests is titled: "Monthly Minister Demographic Survey Reminder" and co-owned by Kevin.Eccles@fmcusa.org and DataProcess@fmcna.org
