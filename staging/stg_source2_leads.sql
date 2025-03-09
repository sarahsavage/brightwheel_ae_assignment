with source2_leads as (select * from {{ref('source2_leads')}})
   , deduplicated_s2_leads as (select *
                                    , row_number() over (partition by "Phone" order by "Address1") as phone_dupe_rank
                               from source2_leads)

select "Type License"                                                                              as license_type
     , "Company"                                                                                   as center_name
     , case when "Accepts Subsidy" = 'Accepts Subsidy' then 1 else 0 end                           as accepts_subsidies
     , case when "Year Round" = 'Year Round' then 1 else 0 end                                     as is_year_round
     , case when "Daytime Hours" = 'Daytime Hours' then 1 else 0 end                               as has_daytime_hours
     , "Star Level"                                                                                as star_level
     , "Mon"                                                                                       as monday_hours
     , "Tues"                                                                                      as tuesday_hours
     , "Wed"                                                                                       as wednesday_hours
     , "Thurs"                                                                                     as thursday_hours
     , "Friday"                                                                                    as friday_hours
     , "Saturday"                                                                                  as saturday_hours
     , "Sunday"                                                                                    as sunday_hours
     , split_part(replace("Primary Caregiver", '\n', ' '), ' ', 1) || ' ' ||
       split_part(replace("Primary Caregiver", '\n', ' '), ' ', 2)                                 as primary_caregiver
     , split_part(replace("Primary Caregiver", '\n', ' '), ' ', 3) || ' ' ||
       split_part(replace("Primary Caregiver", '\n', ' '), ' ', 4)                                 as role
     --would be better to format data in csv before loading
     , regexp_replace("Phone", '[^0-9]', '', 'g')::varchar || case when phone_dupe_rank > 1
                                                              then '-' || phone_dupe_rank::varchar
                                                              else '' end                          as phone
     , md5(regexp_replace("Phone", '[^0-9]', '', 'g')::varchar || case when phone_dupe_rank > 1
                                                                  then '-' || phone_dupe_rank::varchar
                                                                  else '' end)                     as surrogate_key
     , "Email"                                                                                     as email
     , "Address1"                                                                                  as address
     , "Address2"                                                                                  as address2
     , "City"                                                                                      as city
     , "State"                                                                                     as state
     , "Zip"                                                                                       as postal_code
     , "Subsidy Contract Number"                                                                   as subsidy_contract_number
     , "Total Cap"                                                                                 as center_capacity
     , case
           when "Ages Accepted 1" = 'Infants (1-11 months)' or "AA2" = 'Infants (1-11 months)'
               or "AA3" = 'Infants (1-11 months)' or "AA4" = 'Infants (1-11 months)'
               then 1
           else 0 end                                                                              as accepts_infants
     , case
           when "Ages Accepted 1" = 'Toddlers (12-23 months; 1yr.)' or "AA2" = 'Toddlers (12-23 months; 1yr.)'
               or "AA3" = 'Toddlers (12-23 months; 1yr.)' or "AA4" = 'Toddlers (12-23 months; 1yr.)'
               then 1
           else 0 end                                                                              as accepts_toddlers
     , case
           when "Ages Accepted 1" = 'Preschool (24-48 months; 2-4 yrs.)' or "AA2" = 'Preschool (24-48 months; 2-4 yrs.)'
               or "AA3" = 'Preschool (24-48 months; 2-4 yrs.)' or "AA4" = 'Preschool (24-48 months; 2-4 yrs.)'
               then 1
           else 0 end                                                                              as accepts_preschool
     , case
           when "Ages Accepted 1" = 'School-age (5 years-older)' or "AA2" = 'School-age (5 years-older)'
               or "AA3" = 'School-age (5 years-older)' or "AA4" = 'School-age (5 years-older)'
               then 1
           else 0 end                                                                              as accepts_school_age
     , "License Monitoring Since"                                                                  as license_monitoring_since
     , "School Year Only"                                                                          as school_year_only
     , "Evening Hours"                                                                             as evening_hours
from deduplicated_s2_leads
