# Import python packages
import base64
import pandas as pd
import streamlit as st
# from snowflake.snowpark.context import get_active_session
import time

# Write directly to the app
st.title("Process Cashed Checks in System")
st.write(
    """You can use this tool to update check statuses to cashed in the database. These changes will be reflected in Retool and will eventually flow to bubble. 
    """
)


def login():
    st.session_state['authenticated'] = True


def logout():
    st.session_state['authenticated'] = False


if 'authenticated' not in st.session_state:
    st.session_state['authenticated'] = False

if 'authenticated' in st.session_state:
    if st.session_state['authenticated'] == False:
        st.write('login required')
        st.button('Login', on_click=login)
    else:
        st.write('you are logged in')
        st.button('Logout', on_click=logout)
