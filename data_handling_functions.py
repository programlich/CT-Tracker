import pandas as pd
from datetime import datetime, timedelta
import streamlit as st
import json
import gspread
from oauth2client.service_account import ServiceAccountCredentials


def connect_to_docs():
    creds_dict = dict(st.secrets["gcp_service_account"])
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)
    spreadsheet = client.open("CTScanTracker")

    st.session_state["spreadsheet"] = spreadsheet


def create_plan_df(planned_sample):

    spreadsheet = st.session_state["spreadsheet"]
    planned_sample_list = ["sample1_plan", "sample2_plan", "sample3_plan", "sample4_plan", "sample5_plan", "sample6_plan", "sample7_plan", "sample8_plan", "sample9_plan"]

    if planned_sample not in planned_sample_list:
        return f"{planned_sample} is an invalid name. Must be one of:\n {planned_sample_list}"

    start_time = datetime.now() # Experiment starts now
    initial_scantimes = pd.date_range(start=start_time, periods=5, freq="3min") # First 15min -> scan every 3min
    long_term_scantimes = pd.date_range(start=start_time+timedelta(hours=1), periods=24, freq="h")  # after >1h interval = 1h for 24h
    all_scantimes = initial_scantimes.append(long_term_scantimes)
    plan_df = pd.DataFrame({planned_sample: all_scantimes})
    plan_df[planned_sample] = plan_df[planned_sample].dt.strftime("%d.%m.%Y %H:%M:%S") # convert datetime to strings

    # Write plan_df to the corresponding worksheet
    sample_worksheet = spreadsheet.worksheet(planned_sample)

    # Only proceed if worksheet is empty
    records = sample_worksheet.get_all_records()
    if not records:
        sample_worksheet.clear()
        sample_worksheet.update([plan_df.columns.values.tolist()]+plan_df.values.tolist())
        st.session_state["plan_track_df"] = aggregate_plan_and_track_data()
        return f"Sample plan successfully written to worksheet {planned_sample}"

    else:
        return f"Data in worksheet {planned_sample} already exists. No data was written"


def add_scan_to_track_df(tracked_sample):

    spreadsheet = st.session_state["spreadsheet"]
    tracked_sample_list = ["sample1_track", "sample2_track", "sample3_track", "sample4_track", "sample5_track", "sample6_track", "sample7_track", "sample8_track", "sample9_track"]

    if tracked_sample not in tracked_sample_list:
        return f"{tracked_sample} is an invalid name. Must be one of:\n {tracked_sample_list}"

    # Get the current time
    now = datetime.now()
    now_string = now.strftime("%d.%m.%Y %H:%M:%S")

    # Add the current time to the samples record worksheet
    sample_worksheet = spreadsheet.worksheet(tracked_sample)

    # Create time column if it does not exist
    if tracked_sample not in sample_worksheet.row_values(1):
        sample_worksheet.update_cell(1, 1, tracked_sample)

    # Add new timestamp (now) to the time col
    sample_worksheet.append_row([now_string])
    # Reload the plan_track_df
    st.session_state["plan_track_df"] = aggregate_plan_and_track_data()

    return f"{now_string} added to {tracked_sample}"

#@st.cache_data
def aggregate_plan_and_track_data():
    spreadsheet = st.session_state["spreadsheet"]
    plan_df = pd.DataFrame()
    planned_sample_list = ["sample1_plan", "sample2_plan", "sample3_plan", "sample4_plan", "sample5_plan", "sample6_plan", "sample7_plan", "sample8_plan", "sample9_plan"]
    # collect the plan data from all _plan worksheets
    for planned_sample in planned_sample_list:
        sample_worksheet = spreadsheet.worksheet(planned_sample)
        records =  sample_worksheet.get_all_records()
        records_df = pd.DataFrame(records)
        plan_df = pd.concat([plan_df, records_df], axis=1)


    track_df = pd.DataFrame()
    tracked_sample_list = ["sample1_track", "sample2_track", "sample3_track", "sample4_track", "sample5_track", "sample6_track", "sample7_track", "sample8_track", "sample9_track"]
    # collect the plan data from all _plan worksheets
    for tracked_sample in tracked_sample_list:
        sample_worksheet = spreadsheet.worksheet(tracked_sample)
        records =  sample_worksheet.get_all_records()
        records_df = pd.DataFrame(records)
        track_df = pd.concat([track_df, records_df], axis=1)

    plan_track_df = pd.concat([plan_df, track_df], axis=1)

    # Convert values to datetime objects
    time_format = "%d.%m.%Y %H:%M:%S"
    for col in plan_track_df.columns:
        plan_track_df[col] = pd.to_datetime(plan_track_df[col], format=time_format, errors="coerce")


    # Reshape the df into long format
    long_plan_track_df = plan_track_df.melt(var_name="sample", value_name="timestamp")

    # Split the sample_name into 'sample' and 'source' -> plan/track
    long_plan_track_df["source"] = long_plan_track_df["sample"].apply(
        lambda x: "planned" if "_plan" in x else "tracked"
    )
    long_plan_track_df["sample"] = long_plan_track_df["sample"].str.replace("_plan", "")
    long_plan_track_df["sample"] = long_plan_track_df["sample"].str.replace("_track", "")
    long_plan_track_df.sort_values(by="timestamp", inplace=True)

    return long_plan_track_df