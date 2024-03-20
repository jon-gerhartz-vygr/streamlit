from datetime import datetime
from dotenv import load_dotenv
import os
import pandas as pd
import streamlit as st
from utils import execute_pd, update_bubble_thing, execute_query, load_data
from queries import *


load_dotenv()
BUBBLE_KEY = os.getenv("BUBBLE_KEY")


def begin(last_sync_query, get_updates_query):
    st.title("Sync check updates to Bubble Portal")
    st.write(
        """
        You can use this tool to sync all check updates from Snowflake to the Bubble Portal.
        When then job is run, all updates made since the last sync will be applied.
        You can view the proposed updates in the table below.
        """
    )
    st.write(
        """
        1. Load page and review proposed updates
        2. If you believe there are missing updates, reload the page. If they are still not there, ensure the bank_status_updated_ts or mail_status_updated_ts on the check is greater than the last sync date (shown below).
        3. Press "Run" to initiate data sync, the sync will take several minutes to complete. 
        4. Review results and potential errors. 
        """)

    last_sync_df = execute_pd(last_sync_query)

    try:
        last_sync_ts = last_sync_df['sync_ts'][0]
        st.write("Last Sync Timestamp")
        st.write(last_sync_ts)

        updates_df = execute_pd(
            get_updates_query)
        updates_count = len(updates_df.index)

        st.metric("Proposed Update Count", updates_count)
        st.write("Proposed Updates")
        st.write(updates_df)
        return updates_df
    except Exception as e:
        print(e)


def log_success(success_responses):
    success_df = pd.DataFrame.from_records(success_responses)
    with st.spinner("Writing errors to error table, excluding from sync..."):
        try:
            tbl_name = 'BUBBLE_CHECK_SYNC_RECORDS'
            drop_tbl_ddl = drop_tbl.format(tbl_name=tbl_name)
            resp = execute_query(drop_tbl_ddl)
            load_data(success_df, tbl_name, 'STG')
            message = 'errors handled'
        except:
            message = 'failed to handle errors'
        return message


def run_sync(updates_df):
    updates = updates_df.to_dict(orient='records')
    success_responses = []
    fail_responses = []
    with st.spinner("Syncing records to Bubble. This may take a while..."):
        try:
            for i in updates:
                obj_id = i.pop('unique id')
                i['lastUpdated'] = str(i['lastUpdated'])
                resp = update_bubble_thing('check', obj_id, i, BUBBLE_KEY)

                i['unique id'] = obj_id

                if resp.ok:
                    success_responses.append(i)
                else:
                    i['error_message'] = resp.text
                    fail_responses.append(i)
            st.write('✅ Bubble sync complete')
            success_count = len(success_responses)

            st.write(f'Successful records updated: {success_count}')
            failure_count = len(fail_responses)
            total_count = success_count + failure_count
            success_rate = round((success_count/total_count), 1) * 100
            st.write(f'Success Rate: {success_rate}%')
            if failure_count > 0:
                st.write(f'Sync failures: {failure_count}')
                st.write(fail_responses)

            success_resp = log_success(success_responses)
            assert success_resp == 'errors handled', 'failed to handle error transactions, aborting sync.'

            return 'complete'
        except Exception as e:
            st.write('❌ Error syncing records to Bubble')
            print(e.args[0])
            return 'error'


def update_bubble_actual():
    sync_dt = datetime.now()
    sync_ts = sync_dt.strftime('%Y-%m-%d %H:%M:%S.%f')
    with st.spinner("Updating internal bubble actual table..."):
        try:
            q_update_bubble_actual_formatted = q_update_bubble_actual.format(
                sync_ts=sync_ts)
            resp = execute_query(q_update_bubble_actual_formatted)
            status = 'complete'
            st.write('✅ Bubble Actual Table Updated')
        except:
            status = 'failed'
            st.write('❌ Bubble Actual Updates Failed')

    return sync_ts, status


def write_sync_log(sync_ts, sync_status):
    with st.spinner("Writing log in sync log table..."):
        try:
            q_write_sync_log_formatted = q_write_sync_log.format(
                sync_ts=sync_ts, sync_status=sync_status)
            resp = execute_query(q_write_sync_log_formatted)
            st.write('✅ Logs written')
            return resp
        except Exception as e:
            print(e)
            st.write('❌ Failed to write logs')


def snowflake_sync():
    updates_df = begin(q_get_last_sync, q_get_bubble_updates)
    if st.button('Run', key='sync_button'):
        sync_status = run_sync(updates_df)
        if sync_status == 'complete':
            sync_ts, status = update_bubble_actual()
            write_sync_log(sync_ts, status)
