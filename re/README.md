# UPDATE SCRIPT FOR RAISER'S EDGE DATA TO FMCUSA_GL

## Data Source

The primary source for the data updated by this script is a database view export of a series of queries in Raiser's Edge. The source files are as follows:

|filename|description|
|-------------|---------------------------|
|km0017gifts.CSV|export of all gifts in previous cal year to the future, stored procedure updates based on `GL Post Date` minimum/maximum dates.|
|km0020_core_fields.CSV|includes core fields such as deceased flags, name fields, etc.|
|km0021_phones.CSV|includes both phone and email information. No more than one phone and/or one email are listed per constituent record as "primary"|
|km0022_addresses.CSV|includes all addresses for constituent records. Only one address is marked as "primary" per constituent.|
|km0023_constituent_codes.csv|includes any codes added to constituent records, including both a beginning and end date field for each code.|
|km0024_ind_relationships.CSV|one of two relationships files. Blackbaud Raiser's Edge differentiates between relationships with individuals or organizations. Both ind and org files are compiled into the re_relations table in fmcusa_gl|
|km0025_org_relagionship.CSV|one of two relationships files. Blackbaud Raiser's Edge differentiates between relationships with individuals or organizations. Both ind and org files are compiled into the re_relations table in fmcusa_gl|
|km0026_attributes.CSV|similar to constituent codes, but with only a single effective date rather than start/end dates|
|km0027_alias.CSV|aliases are used to give church id numbers and cross-reference those with church constituent records in re. There are also many other uses.|
|km0028_solicit_codes.CSV|various codes used relating to contact preferences|

