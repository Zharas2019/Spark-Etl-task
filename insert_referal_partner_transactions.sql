-- DROP FUNCTION loyalty.f_insert_referral_partner_transactions();

CREATE OR REPLACE FUNCTION loyalty.f_insert_referral_partner_transactions()
 RETURNS void
 LANGUAGE plpgsql
AS $function$
BEGIN

WITH raw_src AS (
  SELECT 
    now() AS change_date, 
    t1.id AS orig_id,
    uuid_in(md5(random()::text || clock_timestamp()::text)::cstring) AS trn_id,
    client.code AS cli_code,
    rfr.last_name||' '||rfr.first_name||' '||rfr.middle_name AS referal_name,
    'arbuz_agent_cashback' AS event_type_name,
    2000 AS cashback_amount,
    ag.id AS customer_id,
    CASE 
      WHEN t1.delivery IS NULL THEN 2 
      WHEN t1.delivery IS TRUE THEN 3
      WHEN t1.delivery IS FALSE OR t1.expired IS TRUE THEN 0 
    END AS loyalty_status
  FROM stage.s16_referral_partner t1
  JOIN stage.s16_customers rfr ON t1.referral_iin = rfr.iin
  JOIN stage.s16_customers ag  ON t1.agent_iin   = ag.iin
  JOIN stage.s01_g_clihst_optim hist
    ON t1.agent_iin = hist.taxcode
   AND current_date BETWEEN hist.fromdate AND hist.todate and hist.typefl='1'
  JOIN stage.s01_g_cli client
    ON client.id = hist.id 
   AND client.dep_id = hist.dep_id
where partner_type = 'arbuz_remote'

  UNION ALL

  SELECT 
    now() AS change_date, 
    t1.id,
    uuid_in(md5(random()::text || clock_timestamp()::text)::cstring) AS trn_id,
    client.code AS cli_code,
    ag.last_name||' '||ag.first_name||' '||ag.middle_name AS referal_name,
    'arbuz_first_cashback' AS event_type_name,
    2000 AS cashback_amount,
    rfr.id,
    CASE 
      WHEN t1.delivery IS NULL THEN 2 
      WHEN t1.delivery IS TRUE THEN 3
      WHEN t1.delivery IS FALSE OR t1.expired IS TRUE THEN 0 
    END AS loyalty_status
  FROM stage.s16_referral_partner t1
  JOIN stage.s16_customers rfr ON t1.referral_iin = rfr.iin
  JOIN stage.s16_customers ag  ON t1.agent_iin   = ag.iin
  JOIN stage.s01_g_clihst_optim hist
    ON t1.referral_iin = hist.taxcode
   AND current_date BETWEEN hist.fromdate AND hist.todate and hist.typefl='1'
  JOIN stage.s01_g_cli client
    ON client.id = hist.id 
   AND client.dep_id = hist.dep_id
where partner_type = 'arbuz_remote'
), src as (SELECT DISTINCT ON (orig_id, customer_id)
         change_date,
         orig_id,
         trn_id,
         cli_code,
         referal_name,
         event_type_name,
         cashback_amount,
         customer_id,
         loyalty_status
  FROM raw_src
  ORDER BY orig_id, customer_id, change_date DESC)

MERGE INTO loyalty.referral_partner_transactions AS tgt
USING src
  ON (tgt.orig_id = src.orig_id AND tgt.customer_id = src.customer_id)

-- update only if status differs (handles NULL safely)
WHEN MATCHED AND (CASE WHEN tgt.loyalty_status IN (1,3,4) THEN 3 ELSE tgt.loyalty_status END)
                    IS DISTINCT FROM src.loyalty_status
  THEN UPDATE SET
    loyalty_status = src.loyalty_status,
    change_date    = src.change_date,
	cli_code       = src.cli_code,
	referal_name   = src.referal_name 

-- insert if no match
WHEN NOT MATCHED THEN
  INSERT (change_date,
          orig_id,
          trn_id,
          cli_code,
          referal_name,
          event_type_name,
          cashback_amount,
          customer_id,
          loyalty_status)
  VALUES (src.change_date,
          src.orig_id,
          src.trn_id,
          src.cli_code,
          src.referal_name,
          src.event_type_name,
          src.cashback_amount,
          src.customer_id,
          src.loyalty_status);

END;
$function$
;
