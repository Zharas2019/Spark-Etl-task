CREATE OR REPLACE VIEW colvir_cdc.referral_history AS 
SELECT trn.cust_id::bigint AS customer_id_hi,
    trn.cust_id_2::bigint AS customer_id,
    trn.refer_type,
    COALESCE(cb.trn_date, trn.invitation_date) AS created_at,
    trn.referral_cashback,
    trn.loyalty_status,
    COALESCE(lref.trn_id, lr.trn_id, trn.trn_id) AS trn_id,
    cb.etn_count,
    cb.pan_number,
        CASE
            WHEN trn.refer_type::text = 'friend_etn_activation'::text THEN lr.fio_agent
            ELSE trn.fio
        END AS fio,
            'REFERRAL'::text AS ref_type
   FROM colvir_cdc.llt_frhc_referral_transactions trn
     LEFT JOIN colvir_cdc.z039_cashback_info cb ON cb.merchant_id::text = trn.trn_id::character varying::text AND cb.rrn::text = 'REFERRAL'::text AND cb.status::text = '11'::text
     LEFT JOIN colvir_cdc.llt_frhc_referral_prg_clients lref ON trn.refer_type::text = 'friends_invitation'::text AND lref.customer_id_hi = trn.cust_id::bigint AND lref.customer_id = trn.cust_id_2::bigint AND lref.is_error = false
     LEFT JOIN colvir_cdc.llt_frhc_referral_prg_clients lr ON trn.refer_type::text = 'friend_etn_activation'::text AND lr.customer_id = trn.cust_id::bigint AND lr.is_error = false
  WHERE 1 = 1 AND trn.loyalty_status = 1
UNION
SELECT lref.customer_id AS customer_id_hi,
    lref.customer_id_hi AS customer_id,
    drt.id AS refer_type,
    lref.created_at,
    drt.referral_cashback,
    3::smallint AS loyalty_status,
    lref.trn_id,
    NULL::numeric AS etn_count,
    NULL::character varying(50) AS pan_number,
    lref.fio_agent AS fio,
            'REFERRAL'::text AS ref_type
   FROM colvir_cdc.llt_frhc_referral_prg_clients lref
     LEFT JOIN colvir_cdc.dict_referral_type drt ON drt.id::text = 'friend_etn_activation'::text
  WHERE 1 = 1 AND lref.is_error = false AND lref.is_cashback_f_activate_etn = false
UNION
SELECT lref.customer_id_hi,
    lref.customer_id,
    drt.id AS refer_type,
    lref.created_at,
    drt.referral_cashback,
    3::smallint AS loyalty_status,
    lref.trn_id,
    NULL::numeric AS etn_count,
    NULL::character varying(50) AS pan_number,
    lref.fio_referral AS fio,
            'REFERRAL'::text AS ref_type
   FROM colvir_cdc.llt_frhc_referral_prg_clients lref
     LEFT JOIN colvir_cdc.dict_referral_type drt ON drt.id::text = 'friends_invitation'::text
  WHERE 1 = 1 AND lref.is_error = false AND lref.is_cashback_f_invite = false
  UNION 
  SELECT  arbuz_refferal.customer_id as customer_id_hi,
    arbuz_refferal.customer_id,
    loyalty_status::character varying AS refer_type,
    arbuz_refferal.change_date AS created_at,
    arbuz_refferal.cashback_amount::numeric(22,3) AS referral_cashback,
    arbuz_refferal.loyalty_status::smallint AS loyalty_status,
    arbuz_refferal.trn_id::uuid,
     cb.etn_count,
    cb.pan_number,
    arbuz_refferal.referal_name::character varying(200) AS fio,
    'ARBUZ'::text AS ref_type
   FROM colvir_cdc.referral_partner_transactions arbuz_refferal
      LEFT JOIN colvir_cdc.z039_cashback_info cb
      ON cb.merchant_id::text = arbuz_refferal.trn_id::character varying::text
      AND cb.rrn::text = 'REFERRAL'::text AND cb.status::text = '11'::text
