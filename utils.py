from dotenv import load_dotenv
import os
import pandas as pd
import requests
import snowflake.connector
import streamlit as st
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import DBAPIError
from snowflake.sqlalchemy import URL

load_dotenv()

SNOWFLAKE_ACCOUNT_ID = os.getenv("SNOWFLAKE_ACCOUNT_ID")
BASE_URL = os.getenv("BASE_URL")
REFRESH_URI = BASE_URL + os.getenv("REFRESH_URI")
BUBBLE_KEY = os.getenv("BUBBLE_KEY")
BUBBLE_BASE_URL = os.getenv("BUBBLE_BASE_URL")
MAX_RETRIES = 3
RETRY_BACKOFF = 1


import certifi, ssl
print("SF connector version:", snowflake.connector.__version__)
print("certifi CA bundle:", certifi.where())
print("OpenSSL:", ssl.OPENSSL_VERSION)


def get_bubble_env():
    if "is_test_mode" not in st.session_state:
        st.session_state["is_test_mode"] = False

    if st.session_state["is_test_mode"] == True:
        return os.getenv("BUBBLE_BASE_URL_TEST")
    else:
        return os.getenv("BUBBLE_BASE_URL")


def connect_to_sf(token, type='session'):
    url_obj = URL(
        account=SNOWFLAKE_ACCOUNT_ID,
        authenticator='oauth',
        token=token,
        database='LIQUIDATION_TRUST',
        SCHEMA='STG'
    )
    engine = create_engine(url_obj, pool_pre_ping=True, pool_recycle=1800)
    Session = sessionmaker(bind=engine)
    session = Session()
    if type == 'session':
        return session
    else:
        return engine


def check_token(token):
    try:
        session = connect_to_sf(token)
        session.execute("SELECT 1")
        return True
    except Exception as e:
        print(e)
        return False


def construct_url(base_url, query_param_name, query_param_val):
    query_string = f"{query_param_name}={query_param_val}"
    full_redirect_url = f"{base_url}?{query_string}"
    return full_redirect_url


def refresh(token):
    url = construct_url(REFRESH_URI, 'token', token)
    resp = requests.get(url)
    resp_data = resp.json()
    if 'error' in resp_data:
        print(resp_data['error'])
    else:
        st.session_state['refresh_token'] = resp_data['refresh_token']
        st.session_state['snowflake_token'] = resp_data['token']


def handle_db_error(e):
    print(e)
    # token_is_valid = check_token(st.session_state['snowflake_token'])
    # if token_is_valid:
    #     message = f'System error: Contact Jonathan: {str(e)}'
    # else:
    #     message = 'login_required'
    #     refresh(st.session_state['refresh_token'])
    message = ''

    return message


def execute_query(query):
    for attempt in range(MAX_RETRIES):
        try:
            with connect_to_sf(st.session_state['snowflake_token']) as session:
                session.execute(query)
                session.commit()
                return 'Query executed'

        except Exception as e:
            message = handle_db_error(e)
            return message


def load_data(data, tbl_name, schema):
    for attempt in range(MAX_RETRIES):
        try:
            engine = connect_to_sf(
                st.session_state['snowflake_token'], type='engine')
            with engine.connect() as conn:
                data.to_sql(tbl_name, conn, schema=schema,
                            index=False, chunksize=16000)
                resp = 'success'
                return resp

        except Exception as e:
            message = handle_db_error(e)
            return message


def execute_pd(query):
    for attempt in range(MAX_RETRIES):
        try:
            with connect_to_sf(st.session_state['snowflake_token']) as session:
                df = pd.read_sql(query, session.bind)
                return df

        except Exception as e:
            message = handle_db_error(e)
            return message


def create_bubble_thing(obj_type, payload):
    headers = {'authorization': f'bearer {BUBBLE_KEY}'}

    BUBBLE_BASE_URL_DYNAMIC = get_bubble_env()
    base_url = f'{BUBBLE_BASE_URL_DYNAMIC}/{obj_type}'

    try:
        resp = requests.post(base_url, headers=headers, json=payload)
        return resp
    except Exception as e:
        print(e)
        return str(e)
