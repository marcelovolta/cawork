/*
===Attribution Panel v 0.2====
---by Marcelo Volta---
---In version control---
*/

--Updates
--Start counting RL referrals only for guides.retirementliving.com (IsReferralAnalizable)
--Improve distinguishing between Invoca and Twilio (IsInvocaCall funcion)
--Converted to a more modular structure. Execution time dropped from 2+ min to 40 secs


--Form parameters
{% form %}

mv_start_date:
  type: date
  default: {{ 'now' | date: '%Y-%m'}}-{{ 'now' | date: '%d' | minus: 1 }}


mv_end_date:
  type: date
  default: {{ 'now' | date: '%Y-%m-%d' }}

{% endform %}

/*
 Definition of a function and Detection of double ConAffID
    Reference: https://docs.aws.amazon.com/redshift/latest/dg/r_CREATE_FUNCTION.html
    Algorithm to find duplicates: https://stackoverflow.com/questions/738282/how-do-you-count-the-number-of-occurrences-of-a-certain-substring-in-a-sql-varch
*/
CREATE FUNCTION CountOccurrencesOfString (varchar, varchar)
      returns int
    stable
    AS $$
      select (LEN($2)-LEN(REPLACE($2,$1,'')))/LEN($1)
    $$ language sql;

CREATE FUNCTION IsInvocaCall (varchar)
      returns bool
    stable
    AS $$
      select ($1 ilike '%transaction_id%' AND $1 ilike '%destination_number%')
    $$ language sql;
 
 --In the next function, the first parameter is object, and the second is data 
 --From RL, we are only analizing referrals coming from guides
 CREATE FUNCTION IsReferralAnalizable (varchar, varchar)
      returns bool
      stable
      AS $$
        select
         (
          ($1 = 'RetirementLiving.com' AND
          $2 ilike '%guides.retirementliving%'
          AND json_extract_path_text($2, 'Host')  <> 'go.retirementliving.com')
          OR
          (coalesce($1,'') <> 'RetirementLiving.com')
          )
      $$ language sql;



  CREATE FUNCTION IsDateInPeriod(date)
      returns bool
      stable
      AS $$
        select
        (
          {% if mv_start_date == '' and mv_end_date == '' %}
            date($1) between CURRENT_DATE - INTERVAL '1 day' AND CURRENT_DATE
          {% else %}
            date($1) between '{{mv_start_date}}' and  '{{mv_end_date}}'
          {% endif %}
          )
        $$ language sql;


  --This function returns true when the referral data field
  --has the necessary values for our analysis
  --returns false only for Twilio calls
  --The first argument is referral_path, the second is data
  CREATE FUNCTION IsDataAnalizable(varchar, varchar)
      returns bool
      stable
      AS $$
        select 
          (
          (($1 = 'form' or $1 = 'click') 
        OR (($1 = 'call') AND IsInvocaCall($2))))       
      $$ language sql;


--Create the temp table with last day referrals
--for all subsequent analysis
create TEMP TABLE daily_referrals as
  (
    select * 
      from etl_output.referral eor
      where
        IsDateInPeriod(eor.submitted_date)
        AND IsReferralAnalizable(eor.object, eor.data)
        AND eor.status = 'Allocated'
    );

select
  date('{{mv_start_date}}') as "start_date", date('{{mv_end_date}}') as "end_date",
  ( --Stat Bv01 Referrals per week
    SELECT count(referrals.id)
      FROM daily_referrals referrals
      ) as Total_Referrals, 
  (-- Stat. Dv01 Count Referrals detected as Bots per week 
    select count(referrals.id) 
      FROM daily_referrals referrals
      where 
        referrals.detected_user_type = 'Bot'
      ) as Bots,
      (
    -- Stat. EFv01 Count Referrals detected as Real Users per week
    select count(referrals.id) 
      FROM daily_referrals referrals
      where 
        referrals.detected_user_type = 'Real'
      ) as Real_User_Referrals,
      (
    -- Stat. Gv01 Count Call Referrals detected as Real Users per week
    select count(referrals.id) from daily_referrals referrals
      where 
        referrals.detected_user_type = 'Real' 
        and referrals.referral_path = 'call') as Call_Referrals,
      (
    -- Stat. Hv01 Count Form Referrals detected as Real Users per week
    select count(referrals.id)from daily_referrals referrals
    where 
        referrals.detected_user_type = 'Real' 
        and referrals.referral_path = 'form') as Form_Referrals, 
    (
    --Stat. Iv01 Count Click Referrals detected as Real Users per week
    select count(referrals.id) from daily_referrals referrals
      where 
        referrals.detected_user_type = 'Real' 
        and referrals.referral_path = 'click') as Click_Referrals, 
      (
    --Stat. Jv01 Count Referrals detected as Real Users with missing ga_client_id per week
    select count(referrals.id) from daily_referrals referrals
      where 
        referrals.detected_user_type = 'Real' 
        and referrals.ga_client_id = '' 
        and IsDataAnalizable(referrals.referral_path, referrals.data)) 
      as Missing_GA_Client_ID,
        (
    /* Stat. Kv01 Count Referrals detected as Real Users with missing ConAffID per week
    No ConAffID assigned but the referrer included a ConAffID
    */
    select count(referrals.id) from daily_referrals referrals
      where 
        referrals.detected_user_type = 'Real' and (coalesce(referrals.conaffid,'') = '' or referrals.conaffid = 'n/a')
        and JSON_EXTRACT_PATH_TEXT(referrals.data, 'referrer') ILIKE '%conaffid%'
        and IsDataAnalizable(referrals.referral_path, referrals.data)) 
      as Missing_ConAffID,
      (
    /* Stat. Lv01 Count Referrals detected as Real Users with double ConAffID per week
    IMPORTANT: This is a count of how many times the ConAffID was received duplicated. The Referrals are fixed afterwards
    so this does not reflect the number of missing ConAffIDs
    */
    select count(referrals.id) from daily_referrals referrals
      where ((CountOccurrencesOfString('conaffid', referrals.data) = 3 
        and CountOccurrencesOfString('referrer', referrals.data) = 1) 
      or (CountOccurrencesOfString('conaffid', referrals.data) = 2 
        and CountOccurrencesOfString('referrer', referrals.data) = 0))
        )  as Double_ConAffID,
        ( /* Stat. Mv01 Count Referrals with no Click ID info
    Missing Click ID in PPC Traffic */
    select count(referrals.id) from daily_referrals referrals
      WHERE ((NULLIF(referrals.conaffid_medium, '')  = 'PPC')) --conaffid medium is PPC
      AND (((NULLIF(referrals.conaffid_source, '')) <> 'Unknown Inorganic (Call)' 
      OR (coalesce(referrals.conaffid_source, '')) = '')) --source is null or is different from Unknown Inorganic (Call)
      AND (referrals.detected_user_type = 'Real')
      AND (json_extract_path_text(referrals.data, 'conaffid', 's') in ('g','b','airp'))
      AND ((LEN((NULLIF(referrals.conaffid_click_id, ''))) < 10) OR coalesce(referrals.conaffid_click_id, '') = '')
      ) as Missing_Click_ID, 
      (  /* Stat. Nv01 Count Referrals detected as Real Users with an invalid ConAffID value set 
    IMPORTANT: This is a count of how many times the ConAffID was received as a non-compliant JSON. The Referrals are fixed afterwords
    so this does not reflect the number of missing ConAffIDs
    Definition of a function and Detection of double ConAffID
    Reference: https://docs.aws.amazon.com/redshift/latest/dg/r_CREATE_FUNCTION.html
    Algorithm to find duplicates: https://stackoverflow.com/questions/738282/how-do-you-count-the-number-of-occurrences-of-a-certain-substring-in-a-sql-varch */

    select count(referrals.id) from daily_referrals referrals
      where ((CountOccurrencesOfString('conaffid', referrals.data) = 2 
        and CountOccurrencesOfString('referrer', referrals.data) = 1) or 
      (CountOccurrencesOfString('conaffid', referrals.data) = 1 
        and CountOccurrencesOfString('referrer', referrals.data) = 0))
      and ((COALESCE(referrals.conaffid, '') =  '') or (referrals.conaffid = 'n/a'))
      AND (referrals.detected_user_type = 'Real') 
      )
      as Non_JSON_Compliant_ConAffID, 
      (
     /* Stat. Ov01 Count Referrals detected as Real Users with missing CA_Session_ID 
    Again, including calls only for SilverBack, for the reasons provided in Iv01 */
    select count(referrals.id) from daily_referrals referrals
      where 
        referrals.detected_user_type = 'Real' 
        and IsDataAnalizable(referrals.referral_path, referrals.data)
        and (coalesce(referrals.ca_session_id,'') = '' or referrals.ca_session_id = 'n/a')
      )
      as Missing_CA_Session_ID, 
       (
     /* Stat. Pv01 Count Referrals detected as Real Users with missing Campaign Name
    Again, including calls only for SilverBack, for the reasons provided in Iv01 */
    select count(referrals.id) from daily_referrals referrals
      where 
        referrals.detected_user_type = 'Real' 
        and ((coalesce(referrals.campaign_name,'') = '' or LEN(referrals.campaign_name) = 0))
      )
      as Missing_Campaign_Name, 
      (
      /* Stat. Qv01 Count Referrals detected as Real Users with missing Object where they don't come from a
      third party website
      Again, including calls only for SilverBack, for the reasons provided in Iv01 */
      select count(referrals.id) from daily_referrals referrals
      where 
        referrals.detected_user_type = 'Real'
        and IsDataAnalizable(referrals.referral_path, referrals.data)
        and coalesce(referrals.object,'') = '' 
        )
      as Missing_Object,
      (
      /* Stat. Pv01 Count Referrals detected as Real Users where the referer is a Local Guide
      and the Affiliate is set to ConsumerAffairs or None
      */
      select count(referrals.id) from daily_referrals referrals
      where 
        IsDataAnalizable(referrals.referral_path, referrals.data)
        and (referrals.data ILIKE '%"reviews.%' OR referrals.data ILIKE '%/reviews.%' OR referrals.data ILIKE '%//reviews.%')
        and (referrals.conaffid_affiliate ilike '%None%'
        or coalesce(referrals.conaffid_affiliate,'')='')
        and referrals.detected_user_type = 'Real'
      )
      as Missing_Affiliate
      
      
      