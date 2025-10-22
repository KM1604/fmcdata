# u_minister_update_reminders.py
### Purpose: Refresh the MS Excel file that powers the reminder email Power Automate Script

## Source
* SQL db - fmcusa_gl on Azure
* Excel file hosted on Sharepoint. Script modifies local file that is then synced using OneDrive app.

## Frequency
As needed, but updating this file is also incorporated in both the ms and cog minister update scripts. Script should not be necessary often.

## Upstream Requirements
Sync set up, Excel file used as source for reminders stored locally, and cfg.toml file updated with file location.

## Downstream Dependencies
Reminder Power Automate Script to send minister update requests. Titled: "Monthly Minister Demographic Survey Reminder" and co-owned by Kevin.Eccles@fmcusa.org and DataProcess@fmcna.org
