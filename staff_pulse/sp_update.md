# Updating Staff Pulse Surveys from MS Forms

This pipeline uses MS forms because being logged in is required to access the form, and employee info can be matched to responses without fuss. Because of this, there are a few special considerations:

* A Power Automate Form takes MS Form responses and enters employee and response data into separate source files on SharePoint.
  * SharePoint path: WMC Team/Documents/ADM - Administration Files/Database Administration/Source Files/staff_pulse
* Teams Approvals are sent to Elizabeth Goodberry. These approvals provide only an email address for the filer and EG provides information about their work status, location, and department.
* These SharePoint files (one with responses, and the other with responder data) are used as the starting point for this py script's pipeline.

## The cfg file
* A working cfg file is saved in the company Bitwarden as a note named sp_cfg.toml - this is also the filename where the script expects to find the cfg file.
* The path to the xlsx files from SharePoint will need to be updated for any new machine used to update staff pulse responses.

## version considerations
* As of 2026-03-17, pywin32 is not ready for python 3.14. For this reason, python 3.13 is used.
* The cfg file specifies the mssql driver for the polars module. If errors are thrown at the df.write_database() step of the script, verify that the driver installed on the machine matches the driver specified in cfg.toml.
