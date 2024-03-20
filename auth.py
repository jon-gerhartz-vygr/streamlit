from dotenv import load_dotenv
import os
import snowflake.connector
import streamlit as st
import requests


load_dotenv()
BASE_URL = os.getenv("BASE_URL")
REFRESH_URI = BASE_URL + os.getenv("REFRESH_URI")


SNOWFLAKE_ACCOUNT_ID = os.getenv("SNOWFLAKE_ACCOUNT_ID")


def connect_to_sf(token):
    conn = snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT_ID,
        authenticator='oauth',
        token=token,
        database='LIQUIDATION_TRUST',
        SCHEMA='STG'
    )
    return conn


def check_token(token):
    try:
        conn = connect_to_sf(token)
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
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


def logout():
    st.session_state['authenticated'] = False


def handle_auth():
    if 'authenticated' not in st.session_state:
        st.session_state['authenticated'] = False

    query_params = st.query_params.to_dict()
    if 'token' in query_params.keys() and 'refresh_token' in query_params.keys():
        token_raw = query_params["token"]
        refresh_token_raw = query_params["refresh_token"]
        refresh_token = refresh_token_raw.replace(' ', '+')
        token = token_raw.replace(' ', '+')
        is_valid_token = check_token(token)

        if is_valid_token:
            st.session_state['snowflake_token'] = token
            st.session_state['refresh_token'] = refresh_token
            st.session_state['authenticated'] = True
        else:
            try:
                refresh(refresh_token)
            except Exception as e:
                print(e)
                st.session_state['authenticated'] = False
