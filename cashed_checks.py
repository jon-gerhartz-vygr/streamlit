# Import python packages
import base64
import pandas as pd
import streamlit as st
import time
from utils import execute_query, load_data
from queries import *


def begin():
    st.title("Process Cashed Checks in System")
    st.write(
        """You can use this tool to update check statuses to cashed in the database. These changes will be reflected in Retool and will eventually flow to bubble.
        Follow the steps below to complete the process:
        """
    )
    st.write(
        """
        1. Upload an RCN file below
        2. Validate data in the preview
        3. Press "Run" to initiate data load
        4. Smile because you just saved a lot of time!
        """)

    st.divider()

    st.header("1. Upload RCN File")
    uploaded_file = st.file_uploader("Cashed Checks RCN File")
    return uploaded_file


def display_upload(uploaded_file):
    st.divider()
    st.header("2. Validate Preview Data")
    try:
        dataframe = pd.read_csv(uploaded_file, sep=' ', header=None)
        rows = len(dataframe.index)

        st.metric("Rows Loaded", rows)

        st.dataframe(
            dataframe[0].head(100),
            hide_index=True,
            column_config={
                "0": st.column_config.Column(
                    "RCN String",
                    width="larger"
                )
            })
        return True, dataframe
    except Exception as e:
        print(e)
        return False, None


def stg_data(show_run, dataframe, tbl_name):
    st.divider()
    st.header("3. Initiate Workflow")
    if show_run:
        if st.button('Run', key='cashed_checks_button'):
            try:
                with st.spinner("Loading data to stage tables..."):
                    drop_tbl_ddl = drop_tbl.format(tbl_name=tbl_name)
                    resp = execute_query(
                        drop_tbl_ddl)
                    st.write('✅ Stage table cleared')
                    schema = 'STG'
                    load_resp = load_data(dataframe, tbl_name, schema)
                    st.write('✅ Stage table loaded')
                return True
            except Exception as e:
                print(e)
                return False


def merge_data(filename):
    try:
        with st.spinner("Merging data to source tables..."):
            insert_checks_paid_formatted = insert_checks_paid.format(
                filename=filename)
            resp = execute_query(insert_checks_paid_formatted)
            st.write('✅ Source table loaded')
        return True

    except Exception as e:
        print(e)
        return False


def log_and_update(filename):
    with st.spinner("Logging data to Audit Log and updating status in USD Distributions..."):
        log_events_formatted = log_events.format(filename=filename)
        resp = execute_query(log_events_formatted)
        st.write('✅ Logs written')

        update_usd_dist_formatted = update_usd_dist.format(filename=filename)
        resp = execute_query(update_usd_dist_formatted)
        st.write('✅ USD Distributions updated')


def cashed_checks():
    uploaded_file = begin()
    show_run, dataframe = display_upload(uploaded_file)

    tbl_name = 'CHECKS_PAID'
    stg_successful = stg_data(show_run, dataframe, tbl_name)
    if stg_successful:
        filename = str(uploaded_file.name)
        merge_data(filename)
        log_and_update(filename)
