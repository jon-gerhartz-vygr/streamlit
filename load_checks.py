import pandas as pd
import streamlit as st
import time
from utils import create_bubble_thing, execute_pd, execute_query, load_data
from queries import *


def begin():
    st.title("Load Checks to Portal")
    st.write(
        """You can use this tool to load reissues and new distribution checks to the customer portal powered by Bubble.io. See the below instructions:
        """
    )

    st.write(
        """
            Manual Steps
            1. Review the checks below, these need to be loaded to Bubble and should already exist there.
            2. Click the "Load Checks" button to start the process. This will do the following:

            Automated Steps
            3. Iterate through the list of checks and load each using the Bubble API (this will take several minutes)
            4. Create a list of Unique IDs which are generated from Bubble as a part of the upload process
            5. Load the checks and Unique IDs to Snowflake in the Bubble Actual table
            6. Provide a list of successful and failed checks

            Note
            1. You can Toggle Test Mode with the switch below, this will upload checks to the dev db in Bubble and a dev table in Snowflake
        """

    )

    is_test_mode = st.toggle("Test Mode")
    st.divider()

    try:
        st.subheader("Pending Checks to be Loaded")

        with st.spinner('Loading pending checks...'):
            checks_to_load_df = execute_pd(
                q_get_new_checks)

        st.dataframe(checks_to_load_df, hide_index=True)

        check_count = len(checks_to_load_df.index)
        total_usd_notional = checks_to_load_df["amount"].sum()
        distinct_check_nums = checks_to_load_df["checkNum"].nunique()

        col1, col2, col3 = st.columns(3)
        col1.metric("Count Checks", f"{check_count:,}")
        col2.metric("Total USD Notional", f"${total_usd_notional:,.2f}")
        col3.metric("Distinct Check Numbers", f"{distinct_check_nums:,}")

        return True, is_test_mode, checks_to_load_df

    except Exception as e:
        print(e)
        st.write("Failed to load checks")
        return False, is_test_mode, pd.Dataframe()


def load_checks_to_bubble(checks_df):
    checks_df['lastUpdated'] = checks_df['lastUpdated'].astype(str)

    if "is_test_mode" not in st.session_state:
        st.session_state["is_test_mode"] = False

    if st.session_state["is_test_mode"] == True:
        checks_df['distribution'] = '1694463850903x464869535615712900'
        checks = checks_df.head(5).to_dict(orient='records')
    else:
        checks = checks_df.to_dict(orient='records')

    successful_uploads = []
    failed_uploads = []

    with st.spinner("Loading checks to bubble"):
        for c in checks:
            resp = create_bubble_thing('check', c)

            if resp.ok:
                json_resp = resp.json()
                checkNum = c['checkNum']
                unique_id = json_resp['id']
                check_map_to_number = {
                    'checkNum': checkNum,
                    'unique id': unique_id,
                }
                successful_uploads.append(check_map_to_number)
            else:
                failed_uploads.append(c['checkNum'])

        all_success = True
        successful_resps = len(successful_uploads)
        failed_checks_df = pd.DataFrame()
        failed_count = len(failed_uploads)

        if failed_count > 0:
            all_success = False
            failed_check_nums_df = pd.DataFrame(
                {'checkNum': failed_uploads})
            failed_checks_df = checks_df.merge(
                failed_check_nums_df, on='checkNum', how='inner')

        return all_success, successful_resps, successful_uploads, failed_checks_df, failed_count


def load_unique_ids_to_snowflake(successful_uploads, checks_df):
    load_to_sf_df = pd.DataFrame(successful_uploads)

    checks_with_unique_ids = checks_df.merge(
        load_to_sf_df, how='inner', on='checkNum')

    try:
        stg_void_tbl_name = 'BUBBLE_CHECK_UPLOADS'
        # drop_tbl comes from the queries.py file
        with st.spinner("Loading unique IDs to Snowflake..."):
            drop_stg_tbl_status = execute_query(
                drop_tbl.format(tbl_name=stg_void_tbl_name))
            load_data_resp = load_data(
                checks_with_unique_ids, stg_void_tbl_name, 'STG')

        with st.spinner("Merging unique IDs to Bubble Actual Table..."):
            if st.session_state["is_test_mode"] == True:
                query = q_merge_loaded_unique_ids_test
            else:
                query = q_merge_loaded_unique_ids

            merge_ids_resp = execute_query(query)
            print(merge_ids_resp)

    except Exception as e:
        st.write("Failed to load unique IDs to snowflake")
        print(e)

    return True


def load_checks_page():
    successfully_loaded_checks, is_test_mode, checks_to_load_df = begin()

    if is_test_mode:
        st.session_state["is_test_mode"] = True

    if successfully_loaded_checks:
        st.write("Step 1 - Load Checks to Bubble")
        st.button('Load Checks', key='load_checks_to_bubble')
        if len(checks_to_load_df.index) > 0:
            all_success, successful_resps, successful_uploads, failed_checks_df, failed_count = load_checks_to_bubble(
                checks_to_load_df)
            if all_success:
                st.success(f"✅ {successful_resps} Checks loaded successfully!")
            else:
                st.warning("Some or all checks failed to upload to Bubble:")
                st.write(f"Success Count: {successful_resps}")
                st.write(f"Fail Count: {failed_count}")
                st.write(f"Failed Checks:")
                st.dataframe(failed_checks_df, hide_index=True)

            if successful_resps > 0:
                is_success = load_unique_ids_to_snowflake(
                    successful_uploads, checks_to_load_df)
                if is_success:
                    st.success(
                        f"✅ Checks synced successfully! Process complete!")
