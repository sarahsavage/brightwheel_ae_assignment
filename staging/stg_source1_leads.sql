with source1_leads as (select * from {{ref('source1_leads')}})
   , deduplicated_s1_leads as (select *
                                    , row_number() over (partition by "Phone" order by "Address") as phone_dupe_rank
                               from source1_leads)

select lower(regexp_replace("Name", '[^a-zA-Z0-9]', '', 'g'))                                     as center_name
     , lower("Credential Type")                                                                   as credential_type
     , "Credential Number"::varchar                                                               as credential_number
     , nullif("Expiration Date",'')::date                                                         as credential_expiration_date
     , case when "Disciplinary Action" = 'Y' then 1
            when "Disciplinary Action" = 'N' then 0
            else null end                                                                         as has_disciplinary_action
     , "Address"                                                                                  as complete_address
     ---this source has address formatted with no comma between street number and city
     ---it is possible to normalize with some complicated logic
     ---might do in future iterations but leaving as is for now, not the primary identifier
     , "State"                                                                                   as state
     , "County"                                                                                  as county
     , regexp_replace("Phone", '[^0-9]', '', 'g')::varchar || case when phone_dup_rank > 1
                                                              then '-' || phone_dup_rank::text
                                                              else '' end                        as phone
     , md5(regexp_replace("Phone", '[^0-9]', '', 'g')::varchar || case when phone_dup_rank > 1
                                                                  then '-' || phone_dup_rank::text
                                                                  else '' end)                   as surrogate_key
     --same comment re: anonymizing/security as other staging models
     , "First Issue Date"::date                                                                  as credential_first_issue_date
     , "Primary Contact"                                                                         as primary_contact
     --some wonky formatting here, sometimes two names listed, some last,first, most first last
     --may want to clean up in future iterations but prefer to force formatting at the source if possible
     , "Primary Contact Role"                                                                    as primary_contact_role
from deduplicated_s1_leads
