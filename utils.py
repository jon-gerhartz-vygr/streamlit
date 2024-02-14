from dotenv import load_dotenv
import os
import pandas as pd
import snowflake.connector
import streamlit as st
from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL

load_dotenv()

SNOWFLAKE_ACCOUNT_ID = os.getenv("SNOWFLAKE_ACCOUNT_ID")


def connect_to_sf(token):
    url_obj = URL(
        account=SNOWFLAKE_ACCOUNT_ID,
        authenticator='oauth',
        token=token,
        database='LIQUIDATION_TRUST',
        SCHEMA='STG'
    )
    engine = create_engine(url_obj)
    return engine


def check_token(token):
    try:
        engine = connect_to_sf(token)
        with engine.connect() as conn:
            conn.execute("SELECT 1")
        return True
    except Exception as e:
        print(e)
        return False


def handle_db_error(e, token):
    token_is_valid = check_token(token)
    if token_is_valid:
        message = f'System error: Contact Jonathan: {str(e)}'
    else:
        message = 'login_required'
    return message


def execute_query(query, token):
    try:
        engine = connect_to_sf(token)
        with engine.connect() as conn:
            conn.execute(query)

        return 'Query executed'

    except Exception as e:
        message = handle_db_error(e, token)
        return message


def load_data(data, tbl_name, schema, token):
    try:
        engine = connect_to_sf(token)
        data.to_sql(tbl_name, engine, schema=schema,
                    index=False, chunksize=16000)
        resp = 'success'
        return resp

    except Exception as e:
        message = handle_db_error(e, token)
        return message
