/*
===Attribution Panel v 0.1====
---by Marcelo Volta---
---In version control---
*/

--Form parameters
{% form %}

mv_start_date:
  type: date
  default: {{ 'now' | date: '%Y-%m'}}-{{ 'now' | date: '%d' | minus: 1 }}


mv_end_date:
  type: date
  default: {{ 'now' | date: '%Y-%m-%d' }}

{% endform %}


CREATE FUNCTION CountOccurrencesOfString (varchar, varchar)
      returns int
    stable
    AS $$
      select (LEN($2)-LEN(REPLACE($2,$1,'')))/LEN($1)
    $$ language sql;

CREATE FUNCTION IsTwilioCall (varchar)
      returns bool
    stable
    AS $$
      select ($1 ilike '%sid%' AND $1 ilike '%variant%')
    $$ language sql;

select
  date('{{mv_start_date}}') as "start_date", date('{{mv_end_date}}') as "end_date",
  ( --Stat Bv01 Referrals per week
    SELECT count(eor.id)
      FROM etl_output.referral eor
      WHERE 
        {% if mv_start_date == '' and mv_end_date == '' %}
          date(eor.submitted_date) between CURRENT_DATE - INTERVAL '1 day' AND CURRENT_DATE
        {% else %}
          date(eor.submitted_date) between '{{mv_start_date}}' and  '{{mv_end_date}}'
        {% endif %}
        AND COALESCE(eor.object, '') <> 'RetirementLiving.com' --RL has been integrated with Leads_API ut it is not sending any analytical information  
        AND eor.status = 'Allocated') as Total_Referrals, 
  (-- Stat. Dv01 Count Referrals detected as Bots per week 
    select count(eor.id) as Column_D from etl_output.referral eor 
      where 
        {% if mv_start_date == '' and mv_end_date == '' %}
          date(eor.submitted_date) between CURRENT_DATE - INTERVAL '1 day' AND CURRENT_DATE
        {% else %}
          date(eor.submitted_date) between '{{mv_start_date}}' and  '{{mv_end_date}}'
        {% endif %}
      and eor.detected_user_type = 'Bot'
      AND COALESCE(eor.object, '') <> 'RetirementLiving.com' --RL has been integrated with Leads_API ut it is not sending any analytical information  )
      ) as Bots,
      (
    -- Stat. EFv01 Count Referrals detected as Real Users per week
    select count(eor.id) from etl_output.referral eor 
      where 
        {% if mv_start_date == '' and mv_end_date == '' %}
          date(eor.submitted_date) between CURRENT_DATE - INTERVAL '1 day' AND CURRENT_DATE
        {% else %}
          date(eor.submitted_date) between '{{mv_start_date}}' and  '{{mv_end_date}}'
        {% endif %}
      and eor.detected_user_type = 'Real'
      AND COALESCE(eor.object, '') <> 'RetirementLiving.com' --RL has been integrated with Leads_API ut it is not sending any analytical information  )
      ) as Real_User_Referrals,
      (
    -- Stat. Gv01 Count Call Referrals detected as Real Users per week
    select count(eor.id) from etl_output.referral eor 
      where 
        {% if mv_start_date == '' and mv_end_date == '' %}
          date(eor.submitted_date) between CURRENT_DATE - INTERVAL '1 day' AND CURRENT_DATE
        {% else %}
          date(eor.submitted_date) between '{{mv_start_date}}' and  '{{mv_end_date}}'
        {% endif %}
      and eor.detected_user_type = 'Real' 
      AND COALESCE(eor.object, '') <> 'RetirementLiving.com' --RL has been integrated with Leads_API ut it is not sending any analytical information  )
      and eor.referral_path = 'call') as Call_Referrals,
      (
    -- Stat. Hv01 Count Form Referrals detected as Real Users per week
    select count(eor.id)from etl_output.referral eor 
    where 
      {% if mv_start_date == '' and mv_end_date == '' %}
        date(eor.submitted_date) between CURRENT_DATE - INTERVAL '1 day' AND CURRENT_DATE
      {% else %}
        date(eor.submitted_date) between '{{mv_start_date}}' and  '{{mv_end_date}}'
      {% endif %}
    and eor.detected_user_type = 'Real' 
    AND COALESCE(eor.object, '') <> 'RetirementLiving.com' --RL has been integrated with Leads_API ut it is not sending any analytical information  )
    and eor.referral_path = 'form') as Form_Referrals, 
    (
    --Stat. Iv01 Count Click Referrals detected as Real Users per week
    select count(eor.id) from etl_output.referral eor 
      where 
        {% if mv_start_date == '' and mv_end_date == '' %}
          date(eor.submitted_date) between CURRENT_DATE - INTERVAL '1 day' AND CURRENT_DATE
        {% else %}
          date(eor.submitted_date) between '{{mv_start_date}}' and  '{{mv_end_date}}'
        {% endif %}
      and eor.detected_user_type = 'Real' 
      AND COALESCE(eor.object, '') <> 'RetirementLiving.com' --RL has been integrated with Leads_API ut it is not sending any analytical information  )
      and eor.referral_path = 'click') as Click_Referrals, 
      (
    /* Stat. Jv01 Count Referrals detected as Real Users with missing ga_client_id per week
    The reason why we count only SilverBack for calls is that SB is the only object type where we use Invoca Numbers
    and thus are able to read a ConAffID value. When we use Invoca in other objects ('Matching Tool', 'Profile', 'Buyers Guide')
    we will need to change the condition for call and include those types of objects as well*/
    select count(eor.id) from etl_output.referral eor 
      where 
        {% if mv_start_date == '' and mv_end_date == '' %}
          date(eor.submitted_date) between CURRENT_DATE - INTERVAL '1 day' AND CURRENT_DATE
        {% else %}
          date(eor.submitted_date) between '{{mv_start_date}}' and  '{{mv_end_date}}'
        {% endif %}
      and eor.status = 'Allocated'
      and eor.detected_user_type = 'Real' 
      and eor.ga_client_id = '' 
      and COALESCE(eor.object, '') <> 'RetirementLiving.com' --RL has been integrated with Leads_API ut it is not sending any analytical information 
      and ((eor.referral_path = 'form' or eor.referral_path = 'click') 
      OR ((eor.referral_path = 'call') AND ((CASE WHEN eor.is_affiliate_object THEN 'My CA' ELSE eor.object END) = 'My CA')) )) 
      as Missing_GA_Client_ID,
        (
    /* Stat. Kv01 Count Referrals detected as Real Users with missing ConAffID per week
    No ConAffID assigned but the referrer included a ConAffID
    Again, including calls only for SilverBack, for the reasons provided in Iv01 */
    select count(eor.id) from etl_output.referral eor 
      where 
        {% if mv_start_date == '' and mv_end_date == '' %}
          date(eor.submitted_date) between CURRENT_DATE - INTERVAL '1 day' AND CURRENT_DATE
        {% else %}
          date(eor.submitted_date) between '{{mv_start_date}}' and  '{{mv_end_date}}'
        {% endif %}
      and eor.status = 'Allocated'
      and eor.detected_user_type = 'Real' and (coalesce(eor.conaffid,'') = '' or eor.conaffid = 'n/a')
      and JSON_EXTRACT_PATH_TEXT(eor.data, 'referrer') ILIKE '%conaffid%'
      AND eor.object <> 'RetirementLiving.com'
      and ((eor.referral_path = 'form' or eor.referral_path = 'click') OR
      ((eor.referral_path = 'call') AND ((CASE WHEN eor.is_affiliate_object THEN '3rd-party Website' ELSE eor.object END) = 'My CA')))) 
      as Missing_ConAffID,
      (
    /* Stat. Lv01 Count Referrals detected as Real Users with double ConAffID per week
    IMPORTANT: This is a count of how many times the ConAffID was received duplicated. The Referrals are fixed afterwords
    so this does not reflect the number of missing ConAffIDs
    Definition of a function and Detection of double ConAffID
    Reference: https://docs.aws.amazon.com/redshift/latest/dg/r_CREATE_FUNCTION.html
    Algorithm to find duplicates: https://stackoverflow.com/questions/738282/how-do-you-count-the-number-of-occurrences-of-a-certain-substring-in-a-sql-varch */
    select count(eor.id) from etl_output.referral eor
      where ((CountOccurrencesOfString('conaffid', eor.data) = 3 and CountOccurrencesOfString('referrer', eor.data) = 1) or 
      (CountOccurrencesOfString('conaffid', eor.data) = 2 and CountOccurrencesOfString('referrer', eor.data) = 0))
      and 
        {% if mv_start_date == '' and mv_end_date == '' %}
          date(eor.submitted_date) between CURRENT_DATE - INTERVAL '1 day' AND CURRENT_DATE
        {% else %}
          date(eor.submitted_date) between '{{mv_start_date}}' and  '{{mv_end_date}}'
        {% endif %}
      and COALESCE(eor.object, '') <> 'RetirementLiving.com')  as Double_ConAffID,
        ( /* Stat. Mv01 Count Referrals with no Click ID info
    Missing Click ID in PPC Traffic */
    select count(referral.id) from etl_output.referral referral 
      WHERE ((NULLIF(referral.conaffid_medium, '')  = 'PPC')) --conaffid medium is PPC or Social (Removed Social)
      AND (((NULLIF(referral.conaffid_source, '')) <> 'Unknown Inorganic (Call)' OR (coalesce(referral.conaffid_source, '')) = '')) --source is null or is different from Unknown Inorganic (Call)
    	AND (referral.detected_user_type = 'Real')
    	AND (json_extract_path_text(referral.data, 'conaffid', 's') in ('g','b','airp'))
    	AND 
    	  {% if mv_start_date == '' and mv_end_date == '' %}
          date(referral.submitted_date) between CURRENT_DATE - INTERVAL '1 day' AND CURRENT_DATE
        {% else %}
          date(referral.submitted_date) between '{{mv_start_date}}' and  '{{mv_end_date}}'
        {% endif %}
    	AND (referral.status = 'Allocated') 
    	AND ((LEN((NULLIF(referral.conaffid_click_id, ''))) < 10) OR coalesce(referral.conaffid_click_id, '') = '')
    	AND COALESCE(referral.object, '') <> 'RetirementLiving.com') as Missing_Click_ID, 
    	(  /* Stat. Nv01 Count Referrals detected as Real Users with an invalid ConAffID value set 
    IMPORTANT: This is a count of how many times the ConAffID was received as a non-compliant JSON. The Referrals are fixed afterwords
    so this does not reflect the number of missing ConAffIDs
    Definition of a function and Detection of double ConAffID
    Reference: https://docs.aws.amazon.com/redshift/latest/dg/r_CREATE_FUNCTION.html
    Algorithm to find duplicates: https://stackoverflow.com/questions/738282/how-do-you-count-the-number-of-occurrences-of-a-certain-substring-in-a-sql-varch */

    select count(eor.id) from etl_output.referral eor
      where ((CountOccurrencesOfString('conaffid', eor.data) = 2 and CountOccurrencesOfString('referrer', eor.data) = 1) or 
      (CountOccurrencesOfString('conaffid', eor.data) = 1 and CountOccurrencesOfString('referrer', eor.data) = 0))
      and ((COALESCE(eor.conaffid, '') =  '') or (eor.conaffid = 'n/a'))
      and COALESCE(eor.object,'') <> 'RetirementLiving.com'
      AND (eor.status = 'Allocated') 
      AND (eor.detected_user_type = 'Real') 
      and 
        {% if mv_start_date == '' and mv_end_date == '' %}
          date(eor.submitted_date) between CURRENT_DATE - INTERVAL '1 day' AND CURRENT_DATE
        {% else %}
          date(eor.submitted_date) between '{{mv_start_date}}' and  '{{mv_end_date}}'
        {% endif %}
        )
      as Non_JSON_Compliant_ConAffID, 
      (
     /* Stat. Ov01 Count Referrals detected as Real Users with missing CA_Session_ID 
    Again, including calls only for SilverBack, for the reasons provided in Iv01 */
    select count(eor.id) from etl_output.referral eor 
      where 
        {% if mv_start_date == '' and mv_end_date == '' %}
          date(eor.submitted_date) between CURRENT_DATE - INTERVAL '1 day' AND CURRENT_DATE
        {% else %}
          date(eor.submitted_date) between '{{mv_start_date}}' and  '{{mv_end_date}}'
        {% endif %}
      and eor.status = 'Allocated'
      and eor.detected_user_type = 'Real' and (coalesce(eor.ca_session_id,'') = '' or eor.ca_session_id = 'n/a')
      AND eor.object <> 'RetirementLiving.com'
      and ((eor.referral_path = 'form' or eor.referral_path = 'click') OR
      ((eor.referral_path = 'call') AND ((CASE WHEN eor.is_affiliate_object THEN '3rd-party Website' ELSE eor.object END) = 'My CA')))
      )
      as Missing_CA_Session_ID, 
       (
     /* Stat. Pv01 Count Referrals detected as Real Users with missing Campaign Name
    Again, including calls only for SilverBack, for the reasons provided in Iv01 */
    select count(eor.id) from etl_output.referral eor 
      where 
        {% if mv_start_date == '' and mv_end_date == '' %}
          date(eor.submitted_date) between CURRENT_DATE - INTERVAL '1 day' AND CURRENT_DATE
        {% else %}
          date(eor.submitted_date) between '{{mv_start_date}}' and  '{{mv_end_date}}'
        {% endif %}
      and eor.status = 'Allocated'
      and eor.detected_user_type = 'Real' 
      and ((coalesce(eor.campaign_name,'') = '' or LEN(eor.campaign_name) = 0))
      AND eor.object <> 'RetirementLiving.com'
      and ((eor.referral_path = 'form' or eor.referral_path = 'click') OR
      ((eor.referral_path = 'call') AND ((CASE WHEN eor.is_affiliate_object THEN 'My CA' ELSE eor.object END) = 'My CA')))
      )
      as Missing_Campaign_Name, 
      (
      /* Stat. Qv01 Count Referrals detected as Real Users with missing Object where they don't come from a
      third party website
      Again, including calls only for SilverBack, for the reasons provided in Iv01 */
      select count(eor.id) from etl_output.referral eor 
      where 
        {% if mv_start_date == '' and mv_end_date == '' %}
          date(eor.submitted_date) between CURRENT_DATE - INTERVAL '1 day' AND CURRENT_DATE
        {% else %}
          date(eor.submitted_date) between '{{mv_start_date}}' and  '{{mv_end_date}}'
        {% endif %}
      AND (eor.status = 'Allocated') 
      AND (eor.detected_user_type = 'Real') 
      AND eor.object <> 'RetirementLiving.com'
      and coalesce(eor.object,'') = '' 
      and not eor.is_affiliate_object
      )
      as Missing_Object,
      (
      /* Stat. Pv01 Count Referrals detected as Real Users where the referer is a Local Guide
      and the Affiliate is set to ConsumerAffairs or None
      */
      select count(eor.id) from etl_output.referral eor 
      where 
        {% if mv_start_date == '' and mv_end_date == '' %}
          date(eor.submitted_date) between CURRENT_DATE - INTERVAL '1 day' AND CURRENT_DATE
        {% else %}
          date(eor.submitted_date) between '{{mv_start_date}}' and  '{{mv_end_date}}'
        {% endif %}
      AND (eor.data ILIKE '%"reviews.%' OR eor.data ILIKE '%/reviews.%' OR eor.data ILIKE '%//reviews.%')
      and eor.status = 'Allocated'
      and eor.conaffid_affiliate ILIKE 'None'
      AND (eor.detected_user_type = 'Real') 
      AND eor.object <> 'RetirementLiving.com'
      )
      as Missing_Affiliate
      
      