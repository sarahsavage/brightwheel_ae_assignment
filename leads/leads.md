{% docs enriched_leads %}

Lead-level data from salesforce and other lead sources. Leads may originate from any of the sources.
Phone number is used as the primary key to match leads and enrich the data on each lead. When a lead does 
not originate from salesforce, the lead_origin field is used to determine which source it came from.
Includes PII and should be handled carefully. 

{% enddocs% }
