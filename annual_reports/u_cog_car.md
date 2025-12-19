# Minister Update Reminders Pipeline
### Purpose: Refresh the MS Excel file that powers the reminder email Power Automate Script

## Source
* Cognito Forms website has entries saved on their site. Local .csv copy of entries is saved to the local folder.
* Form ID, API key, and API key expiration date saved in cfg.toml file

## Frequency
* as needed, more often during church annual reports season (mid-Jan through mid-Feb)
* updates to car should be edited on Cognito and this pipeline run to update SQL db on fmcusa gl

## Upstream Requirements
* API app maintained on Cognito
* edits made to Cognito
* cfg.toml file contains necessary schema changes to Cognito to match fmcusa
* Stored procedure on fmcusa gl contains logic needed to merge formatted source table into live prod table

## Downstream Dependencies
* Power Automate Script reads from SharePoint xlsx file with embedded Power Query to apply logic to determine which ministers need a reminder
* Power Automate Script to send minister update requests is titled: "Monthly Minister Demographic Survey Reminder" and co-owned by Kevin.Eccles@fmcusa.org and DataProcess@fmcna.org
