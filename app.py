import asyncio
from auth import handle_auth, logout
from cashed_checks import cashed_checks
from login import login_page
from process_reissues import process_reissues
import streamlit as st


async def main():
    st.session_state['snowflake_token'] = ''
    await handle_auth()
    if st.session_state['authenticated'] == False:
        st.warning("Please login")

    st.title("Voyager Estate Ops Portal")

    tab1, tab2, tab3 = st.tabs(
        ['Login', 'Cashed Checks', 'Reissue Processing'])

    with tab1:
        st.write("Voyager Streamlit Home Page")
        login_page()

    with tab2:
        cashed_checks()

    with tab3:
        process_reissues()

asyncio.run(main())
