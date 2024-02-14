drop_tbl = """
DROP TABLE IF EXISTS LIQUIDATION_TRUST.STG.{tbl_name}
"""

insert_checks_paid = """
INSERT INTO LIQUIDATION_TRUST.SRC.CHECKS_PAID_FORMATTED
SELECT
    rcn_string
    ,bank_account_number
    ,check_number
    ,pay_status
    ,amount_fmt as amount
    ,citizen_uuid
    ,date_paid_fmt as paid_dt
FROM (
    SELECT
        "0" as rcn_string
        ,left("0", 10) as bank_account_number
        ,right(left("0", 20),8) as check_number
        ,right(left("0", 21),1) as pay_status
        ,right(left("0", 33),12) as amount
        ,left(amount,10) as amt_whole
        ,right(amount,2) as amt_decimal
        ,LTRIM((amt_whole::varchar||'.'||amt_decimal::varchar),0)::float(2)::numeric(18,2) as amount_fmt
        ,left(right("0", 14), 6) as date_paid
        ,left(date_paid,2) as paid_month
        ,right(left(date_paid,4),2) as paid_day
        ,right(date_paid,2) as paid_year
        ,('20'||paid_year||'-'||paid_month||'-'||paid_day)::date as date_paid_fmt
        ,right("0", 8) as citizen_uuid
    FROM LIQUIDATION_TRUST.STG.CHECKS_PAID) rcn_fmt
WHERE rcn_fmt.check_number not in (select check_number FROM LIQUIDATION_TRUST.SRC.CHECKS_PAID_FORMATTED)
"""

log_events = """
INSERT INTO LIQUIDATION_TRUST.SRC.AUDIT_LOG_EVENTS (
"ACTION_TS"
, "ACTION_CODE"
, "IDENTIFIER"
, "IDENTIFIER_TYPE"
, "AUTHOR_NAME"
, "AUTHOR_EMAIL"
, "AUTHOR_IP"
, "AUTHOR_COMMENT"
, "CLIENT_CODE"
, "DATA_BEFORE"
, "DATA_AFTER"
)
SELECT 
    current_timestamp
    , 'UPDATE_CHECK'
    , check_number
    , 'CHECK_NUMBER'
    , 'JonathanGerhartz'
    , 'jgerhartz@investvoyager.com'
    , current_ip_address()
    , 'bulk updating check records from cashed checks RCN file(s): {filename}'
    , 'SNOWFLAKE_WEB_CONSOLE'
    , OBJECT_CONSTRUCT('bank_status', b.bank_status)
    , OBJECT_CONSTRUCT('bank_status', 'CASHED')
FROM (
SELECT a.*
from "LIQUIDATION_TRUST"."SRC"."CHECKS_PAID_FORMATTED" b
JOIN "LIQUIDATION_TRUST"."SRC"."USD_DISTRIBUTIONS" a on a.check_amount = b.check_amount and a.check_number = b.check_number
where 
    a.bank_status <> 'CASHED'
   ) b
"""

update_usd_dist = """
update "LIQUIDATION_TRUST"."SRC"."USD_DISTRIBUTIONS" a
set bank_status = 'CASHED', bank_status_updated_ts = current_timestamp
from "LIQUIDATION_TRUST"."SRC"."CHECKS_PAID_FORMATTED" b
where 
    a.check_number=b.check_number and 
    a.check_amount=b.check_amount and
    a.bank_status <> 'CASHED' 
"""
