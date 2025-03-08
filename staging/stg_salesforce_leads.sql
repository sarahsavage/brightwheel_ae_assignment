with sf_leads as (select * from {{ref('salesforce_leads')}})
   , deduplicated_sf_leads as (select *
                                 , row_number() over (partition by phone order by street) as phone_dupe_rank
                            from sf_leads)

select id                                                                                 as sf_lead_id
     , brightwheel_school_uuid_c                                                          as brightwheel_center_uuid
     , is_deleted
     , lower(first_name)                                                                  as first_name
     , lower(last_name)                                                                   as last_name
     --cleans up some inconsistencies in name formatting
     , lower(title)                                                                       as title
     , lower(regexp_replace(company, '[^a-zA-Z0-9]', '', 'g'))                            as center_name
     --might be overkill but consistent formatting will help match leads downstream
     , street                                                                             as address
     , city
     , state
     , postal_code::varchar
        --allows for alpha-numeric Canadian post codes
     ,country
     --for future iterations will want to add lookup tables for city/state/country mapping
     , phone::varchar || case when phone_dup_rank > 1
                         then '-' || phone_dup_rank::varchar
                         else '' end                                                      as phone
    --just in case there are duplicate phone numbers with same address
     , md5(phone::varchar) || case when phone_dup_rank > 1
                                    then '-' || phone_dup_rank::varchar
                                    else '' end                                          as surrogate_key
     --anonymize phone to create non-pii version of table for wider business user
     --will want a more secure way to anonymize for future iterations
     , mobile_phone::varchar
     , email
     , website
     , lead_source
     , status
     , case when is_converted = 'TRUE' then 1 when is_converted = 'FALSE' then 0
            else null end                                                                as is_converted
     , created_date                                                                      as created_at
     --dbt best practice is to name datetime fields _at to differentiate from date fields
     , nullif(last_modified_date,'')                                                     as last_modified_at
     , nullif(last_activity_date,'')                                                     as last_activity_at
     , nullif(last_viewed_date,'')                                                       as last_viewed_at
     , nullif(last_referenced_date,'')                                                   as last_referenced_at
     , nullif(email_bounced_date,'')                                                     as email_bounced_at
         --these are all blank in the current output, accounting for possibility of empty strings
     , email_bounced_reason
     , outreach_stage_c                                                                  as outreach_stage
     , current_enrollment_c                                                              as current_enrollment
     , capacity_c                                                                        as center_capacity
     , lead_source_last_updated_c                                                        as lead_source_last_updated
from deduplicated_sf_leads


