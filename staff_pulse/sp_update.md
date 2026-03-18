# Updating Staff Pulse Surveys from MS Forms

This pipeline uses MS forms because being logged in is required to access the form, and employee info can be matched to responses without fuss. Because of this, there are a few special considerations:

* A Power Automate Form takes MS Form responses and enters employee and response data into separate source files on SharePoint.
  * SharePoint path: WMC Team/Documents/ADM - Administration Files/Database Administration/Source Files/staff\_pulse
* Teams Approvals are sent to Elizabeth Goodberry. These approvals provide only an email address for the filer and EG provides information about their work status, location, and department.
* These SharePoint files (one with responses, and the other with responder data) are used as the starting point for this py script's pipeline.

## The cfg file
* A working cfg file is saved in the company Bitwarden as a note named sp\_cfg.toml - this is also the filename where the script expects to find the cfg file.
* The path to the xlsx files from SharePoint will need to be updated for any new machine used to update staff pulse responses.

## version considerations
* As of 2026-03-17, pywin32 is not ready for python 3.14. For this reason, python 3.13 is used.
* The cfg file specifies the mssql driver for the polars module. If errors are thrown at the df.write\_database() step of the script, verify that the driver installed on the machine matches the driver specified in cfg.toml.

## fmcusa\_gl
* The pipeline here pushes data to a temporary table, runs a stored procedure on fmcusa\_gl, and then deletes the temp table.
* Employee and response data are stored separately in the temp tables, and the stored procedure matches the employee information with its responses. Only the response id is preserved, which is not matched to employee identity anywhere except in the SharePoint table.
* Lastly, the stored procedure also changes the structure of the answer records so each row is a question response with category, ranking, question, dept, etc. etc. This is a ready-made table able to power the Domo dashboard.

## Domo
* The Domo dashboard is linked here: [https://fmcusa-org.domo.com/page/11602159](https://fmcusa-org.domo.com/page/11602159)
* The Domo dataset is linked here: [https://fmcusa-org.domo.com/datasources/dd0e5c23-a19b-4ee6-9841-4ede8d2973ae/details/overview](https://fmcusa-org.domo.com/datasources/dd0e5c23-a19b-4ee6-9841-4ede8d2973ae/details/overview)
* The Domo dataset must be refreshed manually. It is not part of the weekly refresh schedule since only 4-5 weeks out of the year see updates to the source data.

## Security
* At no point in time should the SharePoint links to the raw data with employee responses be shared with others.
* If access to the data is needed, the Domo dataset is safe to share with internal employees, as responses have been anonymized.
