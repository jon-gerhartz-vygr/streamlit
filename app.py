from auth import handle_auth, logout
from cashed_checks import cashed_checks
from login import login_page
from bubble_sync import snowflake_sync
import streamlit as st

handle_auth()

# MAX_RETRIES = 3
# try:
#     for i in range(0, MAX_RETRIES):
#         if st.session_state['authenticated'] == True:
#             break
#         else:
#             handle_auth()
#     assert st.session_state['authenticated'] == True, 'failed to refresh authentication'
# except AssertionError as e:
#     print(e)
if st.session_state['authenticated'] == False:
    st.warning("Please login")

st.title("Voyager Estate Ops Portal")

tab1, tab2, tab3 = st.tabs(['Login', 'Cashed Checks', 'Bubble Sync'])

with tab1:
    st.write("Voyager Streamlit Home Page")
    login_page()

with tab2:
    cashed_checks()

with tab3:
    snowflake_sync()
