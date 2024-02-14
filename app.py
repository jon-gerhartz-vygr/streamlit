from auth import handle_auth, logout
from cashed_checks import cashed_checks
from login import login_page
import streamlit as st

handle_auth()

if st.session_state['authenticated'] == False:
    st.warning("Please login")

st.title("Voyager Estate Ops Portal")

tab1, tab2 = st.tabs(['Login', 'Cashed Checks'])

with tab1:
    st.write("Voyager Streamlit Home Page")
    login_page()

with tab2:
    cashed_checks()
