# Update Pipeline for importing a table to match postal codes and FIPS county codes to 'postal_and_county' table in fmcusa_gl
### Purpose: enable matching on FIPS county code on queries with only a USPS postal code as src

## Source
* Form ID, API key, and API key expiration date saved in cfg.toml file
* postal_and_county.csv stored on SharePoint: 'WMC Team - Documents\ADM - Administration Files\Database Administration\Source Files\county_codes'
* the stored procedure u_postal_and_county adds a population size ranking to each county code.

## Frequency
* runs nightly - static file does *not* need to be run, but it's computationally cheap
* updates to should be edited on csv source and this pipeline run to update SQL db on fmcusa gl

## Upstream Requirements
* cfg.toml file contains credentials for connecting to fmcusa_gl db with db credentials (not Azure AD)
* Stored procedure on fmcusa gl contains logic needed to merge formatted source table into live prod table

## Downstream Dependencies
* Some heatmap requests require a county FIPS code so Domo can create heatmaps based on county.
* This table enables Domo to pull a county FIPS code from the zip
* Table includes *all* counties per zip. Use county with the highest fraction of population to simplify query logic.
