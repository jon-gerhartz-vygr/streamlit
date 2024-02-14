from auth import logout
from dotenv import load_dotenv
import os
import streamlit as st


load_dotenv()


def login_page():
    BASE_URL = os.getenv("BASE_URL")
    LOGIN_URI = BASE_URL + os.getenv("LOGIN_URI")

    if st.session_state['authenticated'] == False:
        st.write('login required')
        st.link_button('Login', LOGIN_URI)
    else:
        st.write("You are logged in :)")
