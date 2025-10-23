# fmcdata
Data pipelines and documentation for the WMC. Maintained by KM1604/Kevin.Eccles@fmcusa.org

## Structure
Each subfolder is either extra documentation or a self-contained pipeline. Each source-destination pair is kept separate. Sometimes, this rule is flexibly interpreted, such as refreshing all Cognito lookup tables from SQL - they are similar enough that they are all run from the same folder, venv, and scripts.

## Documentation
Documentation is maintained in all subfolders. As a general guideline, each .py file has an associated .md with the same filename. Each script's documentation will include source, general guidelines on frequency, any prerequisites or dependencies, and any quirks that must be addressed.

## *cfg.toml Files
text of all cfg.toml files are available on the FMCUSA IT Bitwarden account under notes. If needed, config and credential files can be rebuilt from there. You may also contact KM (DBA) for a copy of the file.