# Overall Approach:
- Project folder structure aligned as closely as possible to dbt best practices. Intermediate models and specific marts not applicable to the current assignment but could be scaled up at a later date
- Did my best to format sql in Redshift-compatible syntax. My current database is Snowflake.
- Uploaded csv files into seed folder. Assuming these would be extracted directly from the source system (i.e. Salesforce) and therefore would be in a 'sources' folder in real project
- Staging and seed models would not be accessible to business-end users. Assuming role-based schema permission granting is available in the given warehouse 
- Leads folder would eventually house additional models for lead reporting, likely as one of the marts along with additional marts (customers, s
- I did not load my data into a sql database but did upload the csv data into a Google sheet for exploration of the raw data
- QA: count(distinct) and count(*) on specific fields to check for expected values (phone, center name, etc), check for duplicates, and add uniqueness/not_null tests in the .yml file before running dbt test, run final model locally/in user schema to run additional queries against.
- For future iterations, consider implementing monitoring tool such as Monte Carlo or Data Dog to ensure continued data quality and integrity. Would also recommend a tool like fivetran for automated extraction from salesforce into warehouse if data typically comes as a csv and needs to be loaded manually.
- If dbt cloud is a future option, consider implementing a semantic layer to ensure
- Would consider implementing imd models into the pipeline in future iterations as additional sources are added.

# Decisions and Tradeoffs:
- If given more time, I would have uploaded some standard lookup tables for state/country/postal code mapping. Here I did some normalization via lower/regexp_replace but it was minimal
- If addresses need to be matched up, would also explore using Levenshtein distance for street abbreviations like ST/St./St
- Would want to see if there is a possible DE process for normalization of these fields earlier in the pipeline, or if standard formats can be enforced at the point of data entry via settings in sources like Salesforce, Hubspot, Braze, for better automation
- Depending on lead volume as business grows and how often we expect data to change, I would consider an incremental model config. Incremental could also be used to handle specific duplicates but would need to add logic to handle instances where touchpoints have been added to more than one duplicate so as to preserve all touchpoint history
- Some csv file had column names that are not compatible with most databases...fixed in sql but would hope these could be normalized in the extraction process. If leads are always delivered via csv, I would use a text editor to edit column names before loading
- Primary Cargiver field in source2 has odd formatting with newline in same column, assumption in my current code that the data loaded correctly (without misaligning) so coded to clean it up, but a better choice would be to implement a fix at the point of extraction. Again, depends on whether this data comes from a source that can be directly connected to the database or if it will always be a csv upload
- Used coalesce and full outer join for final reporting table but would consider union if I was more familiar with the data and was confident that no touchpoints would be lost in deduplication. Union model is more efficient but coalesce with full join preserves all the data. Depends on volume of leads, warehouse storage space, etc.
- Could include a where clause in each staging model to exclude leads that come in with no phone…assuming all will have them but would we want to exclude if not? Not sure, but some way to signa ‘incomplete lead’ without breaking the model or excluding ones that have no phone #
- Ran out of time but would want to go back and populate column descriptions in the .yml file for the staging models, prioritized the reporting model for this exercise
- Does Brightwheel use the term ‘center’ or ‘school’ when referring to a potential customer facility? Would want to clarify for consistency of field names. For this exercise, I chose ‘center’ and use it in all models
- Depending on how the org handles pii, would think about an anonymized version of the model which could be used for summarizing metrics by region without widely sharing pii. Assuming sales team handles pii reporting correctly. Typically, there is a specific folder in the dbt project for any models containing pii with limited access
- Ideally each set of source data would be named for source, i.e. hubspot_leads, braze_leads, rather than 1,2,3
- Made choices about which fields were relevant for dashboard metrics and limited for readability but would want to consult with salesteam about this as well as partnering with data scientists to run some experiments and determine which factors are most impactful for measurement
- Question: how do we want to handle multiple contacts/outreaches for the same school? Is the lead person-level or school-level? Can we configure salesforce to allow for multiple contact columns: i.e. primary_contact, secondary_contact, etc.?






