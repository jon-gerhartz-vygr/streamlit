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

get_distributions = """
SELECT * 
FROM LIQUIDATION_TRUST.SRC.TRUST_DISTRIBUTIONS;
"""


get_approved_reissue_requests = """
with have_cashed_checks as (
    SELECT distinct claim_number as claim_number
    FROM LIQUIDATION_TRUST.SRC.USD_DISTRIBUTIONS
    WHERE bank_status = 'CASHED'
)
SELECT usd.*, u.id
FROM LIQUIDATION_TRUST.SRC.USD_DISTRIBUTIONS usd
JOIN LIQUIDATION_TRUST.SRC.USER_CLAIM uc on uc.claim_number = usd.claim_number
JOIN LIQUIDATION_TRUST.SRC.USERS u on u.id = uc.user_id
LEFT JOIN LIQUIDATION_TRUST.SRC.TAGS ut on ut.identifier = u.id
LEFT JOIN LIQUIDATION_TRUST.SRC.TAGS ct on ct.identifier = usd.check_number
LEFT JOIN LIQUIDATION_TRUST.SRC.TAGS cat on cat.identifier = uc.claim_number
LEFT JOIN have_cashed_checks hcc on hcc.claim_number = usd.claim_number
WHERE
    usd.distribution_name = '{distribution_name}'
    and (usd.bank_status = 'UNCASHED' OR (usd.bank_status = 'VOID' and usd.mail_status = 'UNDELIVERABLE'))
    and usd.internal_status = 'REISSUE_APPROVED'
    and ut.identifier is null
    and ct.identifier is null
    and cat.identifier is null
    and hcc.claim_number is null
QUALIFY rank() over (partition by usd.claim_number order by check_date desc) = 1;
"""

q_truncate_stg = """
TRUNCATE LIQUIDATION_TRUST.STG.USD_DISTRIBUTIONS_CHECK_CREATION
"""

q_write_check_records_to_stg = """
--create new checks
INSERT INTO LIQUIDATION_TRUST.STG.USD_DISTRIBUTIONS_CHECK_CREATION
with have_cashed_checks as (
    SELECT distinct claim_number as claim_number
    FROM LIQUIDATION_TRUST.SRC.USD_DISTRIBUTIONS
    WHERE bank_status = 'CASHED'
)
, check_series as (
SELECT substr(check_number, 1, 2) as series, max(check_number) as max_check_number
FROM LIQUIDATION_TRUST.SRC.USD_DISTRIBUTIONS
GROUP BY 1
)
, new_check_data as (
SELECT
    usd.distribution_name
    , LPAD(usd.check_number, 9, 0) as check_number
    , substr(usd.check_number, 1, 2) check_number_series
    , substr(usd.check_number, 1, 2)::int + 1 as new_check_number_series
    , cs.max_check_number::int as series_max_check_number
    , substr(series_max_check_number, 3, 6) as series_max_check_id
    , row_number() over (partition by check_number_series order by check_number) as check_batch_id
    , (series_max_check_id + check_batch_id)::int as check_id
    , new_check_number_series::text || check_id::text as new_check_number
    , '{check_date}' as check_date
    , 'UNCASHED' as bank_status
    , current_timestamp as bank_status_updated_ts
    , 'MAILED' as mail_status
    , current_timestamp as mail_status_updated_ts
    , usd.check_amount
    , upper(u.first_name) || ' ' || upper(u.last_name) as full_name
    , upper(REGEXP_REPLACE(
        REGEXP_REPLACE(
            REGEXP_REPLACE(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(
                            REGEXP_REPLACE(
                                REGEXP_REPLACE(
                                    full_name,
                                '[ÀÁÂÃÄÅ]', 'A'),
                            '[àáâãäå]', 'a'),
                        '[ÈÉÊË]', 'E'),
                    '[èéêë]', 'e'),
                '[ÌÍÎÏ]', 'I'),
            '[ìíîï]', 'i'),
        '[ÒÓÔÕÖØ]', 'O'),
    '[òóôõöø]', 'o')) as payee
    , upper(u.address1) as address1
    , upper(u.address2) as address2
    , upper(u.city) as city
    , upper(u.state) as state
    , left(u.zip, 5) as zip
    , case when (u.is_foreign = false OR upper(u.country_code) = 'US' OR u.country_code is null) then 'US' else upper(u.country_code) end as country_code
    , u.is_foreign
    , usd.claim_number
    , '{check_file}' as check_file
    , current_timestamp as updated_at_ts
    , 'ISSUED' as internal_status
    , current_timestamp as internal_status_updated_ts
FROM LIQUIDATION_TRUST.SRC.USD_DISTRIBUTIONS usd
JOIN LIQUIDATION_TRUST.SRC.USER_CLAIM uc on uc.claim_number = usd.claim_number
JOIN LIQUIDATION_TRUST.SRC.USERS u on u.id = uc.user_id
LEFT JOIN LIQUIDATION_TRUST.SRC.TAGS ut on ut.identifier = u.id
LEFT JOIN LIQUIDATION_TRUST.SRC.TAGS ct on ct.identifier = usd.check_number
LEFT JOIN LIQUIDATION_TRUST.SRC.TAGS cat on cat.identifier = uc.claim_number
LEFT JOIN have_cashed_checks hcc on hcc.claim_number = usd.claim_number
JOIN check_series cs on cs.series = check_number_series
WHERE
    usd.distribution_name = '{distribution_name}'
    and (usd.bank_status = 'UNCASHED' OR (usd.bank_status = 'VOID' and usd.mail_status = 'UNDELIVERABLE'))
    and usd.internal_status = 'REISSUE_APPROVED'
    and ut.identifier is null
    and ct.identifier is null
    and cat.identifier is null
    and hcc.claim_number is null
QUALIFY rank() over (partition by usd.claim_number order by check_date desc) = 1)
SELECT 
    distribution_name
    , LPAD(new_check_number, 9, 0) as check_number
    , check_date
    , bank_status
    , bank_status_updated_ts
    , mail_status
    , mail_status_updated_ts
    , check_amount
    , payee
    , address1
    , address2
    , city
    , state
    , zip
    , country_code
    , is_foreign
    , claim_number
    , check_file
    , updated_at_ts
    , internal_status
    , internal_status_updated_ts
FROM new_check_data;
"""

q_create_audit_log_for_check_creation = """
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
, "METADATA"
)
SELECT 
    current_timestamp
    , 'CREATE_CHECK'
    , check_number
    , 'CHECK_NUMBER'
    , 'JonathanGerhartz'
    , 'jgerhartz@investvoyager.com'
    , current_ip_address()
    , 'Creating check files via streamlit resissue automation'
    , 'STREAMLIT'
    , {}::variant
    , {
        'distribution_name': distribution_name
        , 'check_number': check_number
        , 'check_date': check_date
        , 'bank_status': bank_status
        , 'bank_status_updated_ts': bank_status_updated_ts
        , 'mail_status' : mail_status
        , 'mail_status_updated_ts': mail_status_updated_ts
        , 'check_amount': check_amount
        , 'payee': payee
        , 'address1': address1
        , 'address2': address2
        , 'city': city
        , 'state': state
        , 'zip': zip
        , 'country_code': country_code
        , 'is_foreign': is_foreign
        , 'claim_number': claim_number
        , 'check_file': check_file
        , 'updated_at_ts': updated_at_ts
        , 'internal_status': internal_status
        , 'internal_status_updated_ts': internal_status_updated_ts
    }::variant
    , {
        'distribution_name': distribution_name
        , 'check_number': check_number
        , 'check_date': check_date
        , 'bank_status': bank_status
        , 'bank_status_updated_ts': bank_status_updated_ts
        , 'mail_status' : mail_status
        , 'mail_status_updated_ts': mail_status_updated_ts
        , 'check_amount': check_amount
        , 'payee': payee
        , 'address1': address1
        , 'address2': address2
        , 'city': city
        , 'state': state
        , 'zip': zip
        , 'country_code': country_code
        , 'is_foreign': is_foreign
        , 'claim_number': claim_number
        , 'check_file': check_file
        , 'updated_at_ts': updated_at_ts
        , 'internal_status': internal_status
        , 'internal_status_updated_ts': internal_status_updated_ts
    }::variant
FROM LIQUIDATION_TRUST.STG.USD_DISTRIBUTIONS_CHECK_CREATION;
"""

q_copy_check_records_from_stg = """
INSERT INTO LIQUIDATION_TRUST.STG.USD_DISTRIBUTIONS_TEST
SELECT * FROM LIQUIDATION_TRUST.STG.USD_DISTRIBUTIONS_CHECK_CREATION;
"""

q_get_us_check_file_records = """
select '1408673034' as "Originator Account",
to_varchar(a.check_date,'MM/DD/YYYY') as "Check Date",
a.check_number as "Check Number",
b.user_id as "Voyager User ID",
UPPER(a.payee) as "Name of Client",
UPPER(replace(a.address1,',','')) as "Client Mailing Address1",
UPPER(replace(a.address2,',','')) as "Client Mailing Address2",
UPPER(a.city) as "Client City",
UPPER(a.state )as "Client State",
left(a.zip,5) as "Client Zip Code",
a.check_amount as "Balance of Account",
'' as "Void"
--,a.*
from "LIQUIDATION_TRUST"."STG"."USD_DISTRIBUTIONS_TEST" a 
    join "LIQUIDATION_TRUST"."SRC"."USER_CLAIM" b on a.claim_number=b.claim_number
    join "LIQUIDATION_TRUST"."SRC"."USERS" c on b.user_id=c.id
    left join 
        (select identifier as user_id 
            from "LIQUIDATION_TRUST"."SRC"."TAGS"
         where identifier_type = 'USER_ID'
            and tag in ('INVALID_ADDRESS','TRANSFERRED_CLAIM','CLAIM_ON_HOLD','BRG_RECON_LIST','LATE_RETURN_ISSUE_HOLD')) d on c.id=d.user_id
    join "LIQUIDATION_TRUST"."SRC"."CLAIM_SUMMARIES" e on a.claim_number=e.claim_number
where a.bank_status = 'PENDING'
    and coalesce(a.is_foreign,FALSE) = FALSE
    and coalesce(a.address1,'') <> ''
    and d.user_id is null
    and claim_type = 'CUSTOMER'
order by 3;
"""

q_get_foreign_check_file_records = """
with dupe_data as 
(
select '1408673034' as "Originator Account",
to_varchar(a.check_date,'MM/DD/YYYY') as "Check Date",
a.check_number as "Check Number",
b.user_id as "Voyager User ID",
UPPER(a.payee) as "Name of Client",
UPPER(replace(a.zip,',','')) as "Client Mailing Address1",
UPPER(replace(a.address1,',',''))||' '||UPPER(a.city) as "Client Mailing Address2",
UPPER(co.name) as "Client City",
'' as "Client State",
'' as "Client Zip Code",
a.check_amount as "Balance of Account",
'' as "Void", 
row_number () over (partition by check_number order by b.user_id asc) as row_number
--,a.*
from "LIQUIDATION_TRUST"."STG"."USD_DISTRIBUTIONS_TEST" a 
    join "LIQUIDATION_TRUST"."SRC"."USER_CLAIM" b on a.claim_number=b.claim_number
    join "LIQUIDATION_TRUST"."SRC"."USERS" c on b.user_id=c.id
    left join "LIQUIDATION_TRUST"."SRC"."COUNTRY" co on a.country_code=co.code
    left join 
        (select identifier as user_id 
            from "LIQUIDATION_TRUST"."SRC"."TAGS"
         where identifier_type = 'USER_ID'
            and tag in ('TRANSFERRED_CLAIM','CLAIM_ON_HOLD','BRG_RECON_LIST')) d on c.id=d.user_id --'INVALID_ADDRESS',
    join "LIQUIDATION_TRUST"."SRC"."CLAIM_SUMMARIES" e on a.claim_number=e.claim_number
where a.bank_status = 'PENDING'
    and coalesce(a.is_foreign,FALSE) = TRUE
    --and coalesce(a.address1,'') <> ''
    and d.user_id is null
    --and claim_type <> 'CUSTOMER'
order by 3
)
select "Originator Account",
"Check Date",
"Check Number",
"Voyager User ID",
"Name of Client",
"Client Mailing Address1",
"Client Mailing Address2",
"Client City",
"Client State",
"Client Zip Code",
"Balance of Account",
"Void"
from dupe_data 
where row_number = 1
;
"""

q_drop_stg_check_voids = """
DROP TABLE LIQUIDATION_TRUST.STG.STG_CHECK_VOIDS;
"""

q_create_audit_log_for_check_void = """
--insert check update record into auto log
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
    , 'bulk updating check records as a part of automated reissue processing voids.'
    , 'STREAMLIT'
    , {
        'bank_status': b.bank_status,
        'internal_status': b.internal_status
    }::variant
    , {
        'bank_status': 'VOID',
        'internal_status': 'REISSUED'
    }::variant
FROM (
SELECT a.check_number, a.bank_status, a.internal_status
from LIQUIDATION_TRUST.STG.STG_CHECK_VOIDS b
JOIN "LIQUIDATION_TRUST"."SRC"."USD_DISTRIBUTIONS" a on a.check_number = b.check_number
where 
    a.check_number=b.check_number and 
    a.bank_status <> 'VOID' 
   ) b
;
"""


q_void_stg_checks = """
UPDATE "LIQUIDATION_TRUST"."SRC"."USD_DISTRIBUTIONS" a
SET 
    bank_status = 'VOID'
    , bank_status_updated_ts = current_timestamp
    , internal_status = 'REISSUED'
    , internal_status_updated_ts = current_timestamp
FROM LIQUIDATION_TRUST.STG.STG_CHECK_VOIDS b
WHERE 
    a.check_number=b.check_number and 
    a.bank_status <> 'VOID' and
    a.mail_status = 'MAILED';
"""

q_get_voided_check_void_file = """
-- run this and save data to file. Limit to 1000 rows at once
SELECT '1408673034' as "Originator Account",
to_varchar(a.check_date,'MM/DD/YYYY') as "Check Date",
a.check_number as "Check Number",
b.user_id as "Voyager User ID",
    upper(REGEXP_REPLACE(
        REGEXP_REPLACE(
            REGEXP_REPLACE(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(
                            REGEXP_REPLACE(
                                REGEXP_REPLACE(
                                    a.payee,
                                '[ÀÁÂÃÄÅ]', 'A'),
                            '[àáâãäå]', 'a'),
                        '[ÈÉÊË]', 'E'),
                    '[èéêë]', 'e'),
                '[ÌÍÎÏ]', 'I'),
            '[ìíîï]', 'i'),
        '[ÒÓÔÕÖØ]', 'O'),
    '[òóôõöø]', 'o'))
 as "Name of Client",
'' as "Client Mailing Address1",
'' as "Client Mailing Address2",
'' as "Client City",
'' as "Client State",
'' as "Client Zip Code",
a.check_amount::numeric(18,2) as "Balance of Account",
'V' as "Void"
--,a.*
FROM "LIQUIDATION_TRUST"."SRC"."USD_DISTRIBUTIONS" a 
    JOIN "LIQUIDATION_TRUST"."SRC"."USER_CLAIM" b on a.claim_number=b.claim_number
    JOIN "LIQUIDATION_TRUST"."SRC"."USERS" c on b.user_id=c.id
    JOIN LIQUIDATION_TRUST.STG.STG_CHECK_VOIDS d on d.check_number = a.check_number
order by "Check Number" asc
"""

q_write_to_void_file_tracking = """
-- run this to save data from file into void file table
insert into liquidation_trust.src.void_file_tracking 
select 'void_file_2024-06-07_a_v1' -- this is name you gave void file
            as void_file, row_number () over (order by "Check Number") as row_number, 
        * 
from (select '1408673034' as "Originator Account",
to_varchar(a.check_date,'MM/DD/YYYY') as "Check Date",
a.check_number as "Check Number",
b.user_id as "Voyager User ID",
    upper(REGEXP_REPLACE(
        REGEXP_REPLACE(
            REGEXP_REPLACE(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(
                            REGEXP_REPLACE(
                                REGEXP_REPLACE(
                                    a.payee,
                                '[ÀÁÂÃÄÅ]', 'A'),
                            '[àáâãäå]', 'a'),
                        '[ÈÉÊË]', 'E'),
                    '[èéêë]', 'e'),
                '[ÌÍÎÏ]', 'I'),
            '[ìíîï]', 'i'),
        '[ÒÓÔÕÖØ]', 'O'),
    '[òóôõöø]', 'o'))
 as "Name of Client",
'' as "Client Mailing Address1",
'' as "Client Mailing Address2",
'' as "Client City",
'' as "Client State",
'' as "Client Zip Code",
a.check_amount::numeric(18,2) as "Balance of Account",
'V' as "Void"
--,a.*
from "LIQUIDATION_TRUST"."SRC"."USD_DISTRIBUTIONS" a 
    join "LIQUIDATION_TRUST"."SRC"."USER_CLAIM" b on a.claim_number=b.claim_number
    join "LIQUIDATION_TRUST"."SRC"."USERS" c on b.user_id=c.id
where a.mail_status = 'UNDELIVERABLE'
    and a.bank_status <> 'VOID'
order by "Check Number" asc)
order by "Check Number" asc
;
"""
