with sf_leads as (select * from /*{{cref('*/stg_salesforce_leads)/*')}}*/)
   , s1_leads as (select * from /*{{cref('*/stg_source1_leads)/*')}}*/)
   , s2_leads as (select * from /*{{cref('*/stg_source2_leads)/*')}}*/)
   , s3_leads as (select * from /*{{cref('*/stg_source3_leads)/*')}}*/)

select sf_leads.sf_lead_id
     , sf_leads.brightwheel_center_uuid
     , coalesce(sf_leads.surrogate_key, s1_leads.surrogate_key, s2_leads.surrogate_key,
                s3_leads.surrogate_key)                                                                 as surrogate_key
     --assuming salesforce is the main source of truth for lead data
     --would need additional context for other sources to determine which is prioritized next
, sf_leads.lead_source                                                                                  as sf_lead_source
, case when sf_leads.surrogate_key is not null
        and s1_leads.surrogate_key is null
        and s2_leads.surrogate_key is null
        and s3_leads.surrogate_key is null then 'sf_leads'
     when s1_leads.surrogate_key is not null
        and sf_leads.surrogate_key is null
        and s2_leads.surrogate_key is null
        and s3_leads.surrogate_key is null then 'source1_leads'
     when s2_leads.surrogate_key is not null
        and sf_leads.surrogate_key is null
        and s1_leads.surrogate_key is null
        and s3_leads.surrogate_key is null then 'source2_leads'
     when s3_leads.surrogate_key is not null
        and sf_leads.surrogate_key is null
        and s1_leads.surrogate_key is null
        and s2_leads.surrogate_key is null then 'source3_leads'
     when (case when s1_leads.surrogate_key is not null then 1 else 0 end) + 
          (case when s2_leads.surrogate_key is not null then 1 else 0 end) +
          (case when s3_leads.surrogate_key is not null then 1 else 0 end) +
          (case when sf_leads.surrogate_key is not null then 1 else 0 end) > 1  
     then 'Multiple' else 'Unknown'  end                                                                as lead_origin
     --in case new lead sources are added, won't incorrectly attribute
     , coalesce(sf_leads.center_name, s1_leads.center_name, s2_leads.center_name, s3_leads.center_name) as center_name
     , concat(sf_leads.first_name, ' ', sf_leads.last_name)                                             as sf_contact_name
     , title                                                                                            as sf_contact_title
     , sf_leads.email                                                                                   as sf_contact_email
     , s1_leads.primary_contact                                                                         as s1_primary_contact
     , s1_leads.primary_contact_role                                                                    as s1_contact_title
     , s2_leads.primary_caregiver                                                                       as s2_primary_caregiver
     , s2_leads.role                                                                                    as s2_caregiver_role
     , s2_leads.email                                                                                   as s2_contact_email
     , s3_leads.email                                                                                   as s3_center_email
     --these may match but preserving from all sources in case there are multiple contacts from the same center
     , coalesce(sf_leads.address, s1_leads.complete_address, s2_leads.address, s3_leads.address)        as address
     --formats don't always match, more work to be done to normalize/map this field for future iteration
     , coalesce(sf_leads.city, s2_leads.city, s3_leads.city)                                            as city
     --s1_leads format does not include this field
     , coalesce(sf_leads.state, s1_leads.state, s2_leads.state, s3_leads.state)                         as state
     , coalesce(sf_leads.country, 'US')                                                                 as country
     --no other source had this field, assumption is that they are all US-based leads
     , coalesce(sf_leads.phone, s1_leads.phone, s2_leads.phone, s3_leads.phone)                         as phone
     , sf_leads.mobile_phone                                                                            as sf_mobile_phone
     , sf_leads.website
     , coalesce(sf_leads.center_capacity, s2_leads.center_capacity,
                s3_leads.center_capacity)                                                               as center_capacity
     , s1_leads.credential_type
     , s1_leads.credential_expiration_date
     , s3_leads.center_type
     , s2_leads.accepts_subsidies
     , s2_leads.is_year_round
     , s2_leads.star_level
     , coalesce(s2_leads.accepts_infants, s3_leads.is_infant_center, null)                              as accepts_infants
     , coalesce(s2_leads.accepts_toddlers, s3_leads.is_toddler_center,
                null)                                                                                   as accepts_toddlers
     , coalesce(s2_leads.accepts_preschool, s3_leads.is_preschool, null)                                as accepts_preschool
     , coalesce(s2_leads.accepts_school_age, s3_leads.is_school, null)                                  as accepts_school_age
     , coalesce(sf_leads.status, 'New Lead')                                                            as lead_status
     , coalesce(sf_leads.is_converted, 0)                                                               as is_converted
     , sf_leads.created_at                                                                              as sf_lead_created_at
     , sf_leads.last_modified_at                                                                        as sf_lead_modified_at
     , sf_leads.last_activity_at                                                                        as sf_lead_last_activity_date
     , sf_leads.lead_source_last_updated
     , coalesce(sf_leads.outreach_stage, 'No Outreach')                                                 as outreach_stage
--assume that null values in sf_leads indicate no outreach and that new leads from other sources have not been outreached
from sf_leads
         full outer join s1_leads on sf_leads.surrogate_key = s1_leads.surrogate_key
         full outer join s2_leads on s1_leads.surrogate_key = s2_leads.surrogate_key
         full outer join s3_leads on s2_leads.surrogate_key = s3_leads.surrogate_key
where is_deleted = 'FALSE' or is_deleted is null
--only want active leads from salesforce
