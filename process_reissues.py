import pandas as pd
import streamlit as st
import time
from utils import execute_query, execute_pd, load_data
from queries import *


def begin():
    st.title("Process Approved Reissue Requests")
    st.write(
        """You can use this tool to process checks that have been approved for reissue. Completing the below flow will initiate the following steps:
        """
    )
    st.write(
        """
        1. Display population of eligible checks for reissue processing
        2. Create new check records using most up to date address and PII information
        3. Add records to check file and export check file CSV.
        4. Void previous check, update internal status to "REISSUED", create void file and export void file to CSV.
        """)

    st.divider()
    try:
        distributions_df = execute_pd(get_distributions)
        distributions = distributions_df['name'].values

        distribution_name = st.selectbox(
            "Select a distribution to review pending resissue requests",
            distributions)

        st.subheader("Pending Eligible Reissue Requests")
        formatted_get_approved_reissue_requests = get_approved_reissue_requests.format(
            distribution_name=distribution_name)
        eligible_reissues_df = execute_pd(
            formatted_get_approved_reissue_requests)
        return True, eligible_reissues_df, distribution_name, ''
    except Exception as e:
        print(e)
        return False, '', '', e


def display_pop(eligible_reissues_df):
    st.write(eligible_reissues_df)


def create_check_records(distribution_name, check_date, check_file):
    try:
        truncate_stg_status = execute_query(q_truncate_stg)
        q_write_check_records_to_stg_formatted = q_write_check_records_to_stg.format(
            distribution_name=distribution_name, check_date=check_date, check_file=check_file)
        create_stg_records_status = execute_query(
            q_write_check_records_to_stg_formatted)
        create_audit_log_status = execute_query(
            q_create_audit_log_for_check_creation)
        create_check_records_status = execute_query(
            q_copy_check_records_from_stg)
    except Exception as e:
        error_message = str(e.args[0])
        create_check_records_status = f'Failed to create check records {error_message}'
    return create_check_records_status


def create_check_file(is_foreign):
    try:
        if is_foreign:
            check_file_df = execute_pd(q_get_foreign_check_file_records)
        else:
            check_file_df = execute_pd(q_get_us_check_file_records)

        return True, check_file_df, ''
    except Exception as e:
        return False, '', e


def void_old_checks(eligible_reissues_df):
    try:
        stg_void_tbl_name = 'STG_CHECK_VOIDS'
        stg_void_schema_name = 'STG'
        drop_stg_status = execute_query(
            drop_tbl.format(tbl_name=stg_void_tbl_name))

        print(eligible_reissues_df.head())
        void_checks_df = eligible_reissues_df['check_number']

        load_data_resp = load_data(
            void_checks_df, stg_void_tbl_name, stg_void_schema_name)

        print(load_data_resp)

        # just need to make sure I can actually void these without messing things up
        # void_checks_resp = execute_query(q_void_stg_checks)
        void_checks_resp = ''
    except Exception as e:
        error_message = str(e.args[0])
        void_checks_resp = f'Failed to void checks {e}'
    return void_checks_resp


def create_void_file():
    try:
        void_file_df = execute_pd(q_get_voided_check_void_file)
        return True, void_file_df, ''
    except Exception as e:
        print(e)
        return False, '', str(e.args[0])


def process_reissues():
    Success, eligible_reissues_df, distribution_name, message = begin()
    if Success:
        display_pop(eligible_reissues_df)
        st.write("")
        st.subheader(
            "To process the above population, choose a check date and create a name for the check file.")

        check_date = st.date_input("Check date")
        check_file = st.text_input(
            "Check file name", "check_file_reissue...")

        run_button = st.button('Run', key='run_resissue_flow_button')
        if run_button:
            # with st.spinner('Creating check records in USD Distributions...'):
            # create_records_status = create_check_records(
            #     distribution_name, check_date, check_file)

            # if create_records_status == 'Query executed':
            #     st.write('✅ Check records created')
            # else:
            #     st.write('❌ Writing check records failed')
            #     print(create_records_status)

            with st.spinner('Creating check files...'):
                is_foriegn_list = [True, False]
                for is_foreign in is_foriegn_list:
                    Success, check_file_df, message = create_check_file(
                        is_foreign)
                    if is_foreign == True:
                        check_file_type = 'foreign check file'
                    else:
                        check_file_type = 'US check file'

                    if Success:
                        st.write(f'✅ Successfully generated {check_file_type}')
                        st.write(check_file_df)
                    else:
                        st.write(f'❌ Failed to create {check_file_type}')

            with st.spinner('Voiding old checks...'):
                void_status = void_old_checks(eligible_reissues_df)
                Success, void_file_df, error_message = create_void_file()
                if Success:
                    st.write(f'✅ Successfully generated void file')
                    st.write(void_file_df)
                    st.write('✅ Old checks voided')
                    st.write('✅ Reissue processing complete')
                else:
                    st.write(f'❌ Failed to create void file: {error_message}')
    else:
        st.write(message)
