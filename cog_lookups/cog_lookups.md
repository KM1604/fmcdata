# Cognito Forms Lookup Refresh
### Purpose: pull various tables from fmcusa gl over to Cognito Forms

## Source
* SQL db - fmcusa_gl on Azure

## Frequency
* As needed, whenever the forms need to be updated

## Upstream Requirements
* cfg.toml file updated with file location
* fmcusa gl SQL db on Azure provides the source data

## Downstream Dependencies
* lookup tables used in various Cognito Forms