with s3_leads as (select * from {{ref('source3_leads')}})
   , deduplicated_s3_leads as (select *
                                    , row_number() over (partition by "Phone" order by "Address") as phone_dupe_rank
                               from source3_leads)

select "Operation"                                                                                  as operation_id
     , "Agency Number"                                                                              as agency_number
     --currently all null but leaving for future iterations
     , lower(regexp_replace("Operation Name", '[^a-zA-Z0-9]', '', 'g'))                             as center_name
     , "Address"                                                                                    as address
     , "City"                                                                                       as city
     , "State"                                                                                      as state
     , "Zip"::varchar	                                                                            as postal_code
    --allowing for Canadian, alphanumeric postal codes
    ,"County" as county
     , regexp_replace("Phone", '[^0-9]', '', 'g')::varchar || case when phone_dupe_rank > 1
                                                              then '-' || phone_dupe_rank::varchar
                                                              else '' end                           as phone
    , md5(regexp_replace("Phone", '[^0-9]', '', 'g')::varchar || case when phone_dupe_rank > 1
                                                                  then '-' || phone_dupe_rank::varchar
                                                                  else '' end)                      as surrogate_key
     --anonymizing to limit PII in downstream tables
     , "Type"                                                                                       as center_type
     , "Status"                                                                                     as status
     , "Issue Date"::date                                                                           as issue_date
    --might be the same as first_issue_date in other table but would need to validate
     , "Capacity" as center_capacity
     , "Email Address"                                                                              as email
     , "Facility ID"                                                                                as facility_id
     , "Monitoring Frequency"                                                                       as monitoring_frequency
     , case
           when "Infant" = 'Y' then 1
           when "Infant" = 'N' then 0
           else null end                                                                            as is_infant_center
     , case
           when "Toddler" = 'Y' then 1
           when "Toddler" = 'N' then 0
           else null end                                                                            as is_toddler_center
     , case
           when "Preschool" = 'Y' then 1
           when "Preschool" = 'N' then 0
           else null end                                                                            as is_preschool
     , case
           when "School" = 'Y' then 1
           when "School" = 'N' then 0
           else null end                                                                            as is_school
from deduplicated_s3_leads

