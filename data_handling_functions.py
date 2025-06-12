import time
import sqlite3
import pandas as pd
from datetime import datetime, timedelta
import streamlit as st
import json
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os
from zoneinfo import ZoneInfo
import samples


# Connect to local sqlite. Create it if it does not exist
def establish_db_connection():
    connection = sqlite3.connect("scans.sqlite", check_same_thread=False)
    cursor = connection.cursor()

    # Create a minimal placeholder table (optional, safe)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS plan_track (
            id INTEGER PRIMARY KEY AUTOINCREMENT
        )
    ''')
    connection.commit()

    return connection

@st.dialog("Delete All Data")
def delete_dialog():
    st.error("Caution! THIS WILL DELETE ALL DATA! EVERYTHING WILL BE LOST IF YOU DONT HAVE A BACKUP!")
    st.warning("Proceed?")
    cols = st.columns([0.8, 0.2])
    text = st.text_input("Type 'delete all data' to proceed")
    if text == "delete all data":
        delete_db()

def delete_db():
    if os.path.exists("scans.sqlite"):
        if "plan_track_df" in st.session_state:
            del st.session_state["plan_track_df"]
        os.remove("scans.sqlite")
        st.rerun()


def get_plan_track_table():
    connection = establish_db_connection()
    plan_track_df = pd.read_sql("SELECT * FROM plan_track", connection)
    connection.close()
    return  plan_track_df


def get_db_table_as_df(sample):
    connection = establish_db_connection()
    try:
        plan_track_df = pd.read_sql(f"SELECT * FROM {sample}", connection)
    except (pd.io.sql.DatabaseError, sqlite3.OperationalError) as e:
        if "no such table" in str(e).lower():
            plan_track_df = None
        else:
            raise  # re-raise if it's a different error
    finally:
        connection.close()
    return plan_track_df


def format_plan_track_table():
    connection = establish_db_connection()

    plan_track_df = get_plan_track_table()

    # Drop the id col, if it exists
    if "id" in plan_track_df.columns:
        plan_track_df.drop(columns=["id"], inplace=True)

    # Convert values to datetime objects
    time_format = "%d.%m.%Y %H:%M:%S%z"
    for col in plan_track_df.columns:
        plan_track_df[col] = pd.to_datetime(plan_track_df[col], format=time_format, errors="coerce", utc=True).dt.tz_convert("Europe/Berlin")

    # Reshape the df into long format
    long_plan_track_df = plan_track_df.melt(var_name="sample", value_name="timestamp")

    if not long_plan_track_df.empty:
        # Split the sample_name into 'sample' and 'source' -> plan/track
        long_plan_track_df["source"] = long_plan_track_df["sample"].apply(
            lambda x: "planned" if "_plan" in x else "tracked"
        )
        long_plan_track_df["sample"] = long_plan_track_df["sample"].str.replace("_plan", "")
        long_plan_track_df["sample"] = long_plan_track_df["sample"].str.replace("_track", "")
        long_plan_track_df.sort_values(by="timestamp", inplace=True)

    connection.close()
    return long_plan_track_df


def create_new_sag_in_db(sag_sample):
    # connect to db
    connection = establish_db_connection()
    cursor = connection.cursor()

    # get sample information
    sample_info = samples.sag_samples[sag_sample]
    interval_list = sample_info["intervals"]
    T_list = sample_info["T"]

    # Create the table with correct column types
    cursor.execute(f'''
    CREATE TABLE IF NOT EXISTS {sag_sample} (
        interval INTEGER,
        t_start_target TEXT,
        t_end_target TEXT,
        t_start_is TEXT,
        t_end_is TEXT,
        T INTEGER
    )
    ''')

    # Insert values into the table: only 'interval' is set, others remain NULL
    data_to_insert = list(zip(interval_list, T_list))
    cursor.executemany(f'''
    INSERT INTO {sag_sample} (interval, t_start_target, t_end_target, t_start_is, t_end_is, T)
    VALUES (?, NULL, NULL, NULL, NULL, ?)
    ''', data_to_insert)

    # Commit and close
    connection.commit()
    connection.close()


def start_next_leaching_interval(sag_sample):
    # connect to db
    connection = establish_db_connection()
    cursor = connection.cursor()

    # Get the ROWID and interval of the first row with NULL t_start_target
    cursor.execute(f'''
        SELECT ROWID, interval FROM {sag_sample}
        WHERE t_start_target IS NULL
        ORDER BY ROWID ASC
        LIMIT 1
    ''')
    result = cursor.fetchone()

    if result is None:
        connection.close()
        return None  # No more intervals to process

    rowid, next_interval = result

    # Define the scantimes
    start_time = datetime.now(ZoneInfo("Europe/Berlin")) # Experiment starts now
    end_time = start_time+timedelta(minutes=next_interval) # First 15min -> scan every 3min
    start_time_string = start_time.strftime("%d.%m.%Y %H:%M:%S%z")
    end_time_string = end_time.strftime("%d.%m.%Y %H:%M:%S%z")


    # Update the table with new timestamps
    cursor.execute(f'''
        UPDATE {sag_sample}
        SET t_start_target = ?, t_end_target = ?
        WHERE ROWID = ?
    ''', (start_time_string, end_time_string, rowid))

    connection.commit()
    connection.close()


def add_leaching_start_time(sample):
    # Connect to DB
    connection = establish_db_connection()
    cursor = connection.cursor()

    # Get the ROWID of the first row with NULL t_start_is
    cursor.execute(f'''
        SELECT ROWID FROM {sample}
        WHERE t_start_is IS NULL
        ORDER BY ROWID ASC
        LIMIT 1
    ''')
    result = cursor.fetchone()

    if result is None:
        connection.close()
        return None  # No unmarked rows found

    rowid = result[0]

    # Generate the current timestamp
    timestamp = datetime.now(ZoneInfo("Europe/Berlin")).strftime("%d.%m.%Y %H:%M:%S%z")

    # Update the row with the timestamp
    cursor.execute(f'''
        UPDATE {sample}
        SET t_start_is = ?
        WHERE ROWID = ?
    ''', (timestamp, rowid))

    connection.commit()
    connection.close()

    return {
        "rowid": rowid,
        "t_start_is": timestamp
    }


def add_leaching_end_time(sample):
    # Connect to DB
    connection = establish_db_connection()
    cursor = connection.cursor()

    # Get the ROWID of the first row with NULL t_start_is
    cursor.execute(f'''
        SELECT ROWID FROM {sample}
        WHERE t_end_is IS NULL
        ORDER BY ROWID ASC
        LIMIT 1
    ''')
    result = cursor.fetchone()

    if result is None:
        connection.close()
        return None  # No unmarked rows found

    rowid = result[0]

    # Generate the current timestamp
    timestamp = datetime.now(ZoneInfo("Europe/Berlin")).strftime("%d.%m.%Y %H:%M:%S%z")

    # Update the row with the timestamp
    cursor.execute(f'''
        UPDATE {sample}
        SET t_end_is = ?
        WHERE ROWID = ?
    ''', (timestamp, rowid))

    connection.commit()
    connection.close()

    return {
        "rowid": rowid,
        "t_end_is": timestamp
    }


def get_total_sag_df(table_names):

    connection = establish_db_connection()

    try:
        # Read from sample20 and tag source
        df1 = pd.read_sql("SELECT * FROM sample20", connection)
        df1["sample"] = "sample20"

        # Read from sample21 and tag source
        df2 = pd.read_sql("SELECT * FROM sample21", connection)
        df2["sample"] = "sample21"

        # Concatenate vertically
        combined_df = pd.concat([df1, df2], axis=0, ignore_index=True)

    except (pd.io.sql.DatabaseError, sqlite3.OperationalError) as e:
        print(f"Error reading tables: {e}")
        combined_df = None

    finally:
        connection.close()

    return combined_df


def format_sag_df(sag_df):

    if not sag_df.empty:
        time_cols = ["t_start_target", "t_end_target", "t_start_is", "t_end_is"]
        time_format = "%d.%m.%Y %H:%M:%S%z"
        for col in time_cols:
            sag_df[col] = pd.to_datetime(sag_df[col], format=time_format, errors="coerce",
                                              utc=True).dt.tz_convert("Europe/Berlin")

        # Drop all T and interval cols
        sag_df = sag_df[["sample", "t_start_target", "t_end_target", "t_start_is", "t_end_is"]]

        # Reshape the df into long format
        # Define columns to melt
        timestamp_cols = ["t_start_is", "t_end_is", "t_start_target", "t_end_target"]

        # Melt the DataFrame
        long_sag_df = pd.melt(
            sag_df,
            id_vars=["sample"],
            value_vars=timestamp_cols,
            var_name="source",
            value_name="timestamp"
        )

        if not long_sag_df.empty:
            long_sag_df["source"] = long_sag_df["source"].apply(lambda s: "planned" if "target" in s else "tracked" if "is" in s else None)

            # # Split the sample_name into 'sample' and 'source' -> plan/track
            # long_sag_df["source"] = long_sag_df["sample"].apply(
            #     lambda x: "planned" if "_plan" in x else "tracked"
            # )
            # long_sag_df["sample"] = long_sag_df["sample"].str.replace("_plan", "")
            # long_sag_df["sample"] = long_sag_df["sample"].str.replace("_track", "")
            long_sag_df.sort_values(by="timestamp", inplace=True)

        return long_sag_df

    else:
        return None


# Add a plan_df to the db as a new column
def add_plan_df_to_db(sample):
    connection = establish_db_connection()
    planned_sample = f"{sample}_plan"
    sample_info = samples.samples[sample]

    cursor = connection.cursor()

    planned_sample_list = ["sample1_plan", "sample2_plan", "sample3_plan", "sample4_plan", "sample5_plan", "sample6_plan", "sample7_plan", "sample8_plan", "sample9_plan"]

    if planned_sample not in planned_sample_list:
        st.toast(f"{planned_sample} is an invalid name. Must be one of:\n {planned_sample_list}")
        connection.close()
        return

    # Create empty column for planned_sample
    try:
        cursor.execute(f"ALTER TABLE plan_track ADD COLUMN {planned_sample} TEXT")
        connection.commit()
    except sqlite3.OperationalError as e:
        if "duplicate column name" in str(e):
            st.toast(f"Column for {planned_sample} already exists. No data was written")
            connection.close()
            return
        else:
            connection.close()
            raise e

    # Get information about this sample from samples.py
    duration = sample_info["duration"]
    if "inital_repetitions" in sample_info:
        initial_reps = sample_info["inital_repetitions"]
    else:
        initial_reps = 1

    # Define the scantimes
    start_time = datetime.now(ZoneInfo("Europe/Berlin")) # Experiment starts now
    initial_scantimes = pd.date_range(start=start_time, periods=initial_reps, freq="20min") # First 15min -> scan every 3min
    long_term_scantimes = pd.date_range(start=start_time+timedelta(hours=1), periods=duration, freq="h")  # after >1h interval = 1h for 24h
    all_scantimes = initial_scantimes.append(long_term_scantimes)
    plan_times = all_scantimes.strftime("%d.%m.%Y %H:%M:%S%z").tolist()

    # Write to the db
    # Get the current number of rows in plan_track
    cursor.execute("SELECT COUNT(*) FROM plan_track")
    row_count = cursor.fetchone()[0]

    # Add blank rows to plan_track, if plan_times is longer then the current table
    if row_count < len(plan_times):
        rows_to_add = len(plan_times) - row_count
        cursor.executemany("INSERT INTO plan_track DEFAULT VALUES", [()] * rows_to_add)
        connection.commit()

    # Update the new column row by row with the values from plan_times
    for i, time in enumerate(plan_times):
        cursor.execute(f"UPDATE plan_track SET {planned_sample} = ? WHERE rowid = ?", (time, i+1))
    connection.commit()

    st.session_state["plan_track_df"] = format_plan_track_table()

    connection.close()
    st.rerun()


def add_scan_to_db(tracked_sample):
    connection = establish_db_connection()
    cursor = connection.cursor()
    tracked_sample_list = ["sample1_track", "sample2_track", "sample3_track", "sample4_track", "sample5_track", "sample6_track", "sample7_track", "sample8_track", "sample9_track"]

    if tracked_sample not in tracked_sample_list:
        connection.close()
        return f"{tracked_sample} is an invalid name. Must be one of:\n {tracked_sample_list}"

    # Get the current time
    now = datetime.now(ZoneInfo("Europe/Berlin"))
    now_string = now.strftime("%d.%m.%Y %H:%M:%S%z")

    # Add the current time to the samples record worksheet
    # Step 1: Check if column exists
    cursor.execute("PRAGMA table_info(plan_track)")
    existing_columns = [row[1] for row in cursor.fetchall()]

    # Step 2: Add column if it doesn't exist
    if tracked_sample not in existing_columns:
        cursor.execute(f"ALTER TABLE plan_track ADD COLUMN {tracked_sample} TEXT")
        connection.commit()

    # Find first empty row in the column
    cursor.execute(f"""
           SELECT rowid FROM plan_track
           WHERE {tracked_sample} IS NULL
           ORDER BY rowid ASC
           LIMIT 1
       """)
    row = cursor.fetchone()
    if row:  # If an empty slot exists
        rowid = row[0]
        cursor.execute(f"""
                UPDATE plan_track SET {tracked_sample} = ?
                WHERE rowid = ?
            """, (now_string, rowid))
    else:  # No empty row — append a new row
        cursor.execute(f"""
                INSERT INTO plan_track ({tracked_sample}) VALUES (?)
            """, (now_string,))

    connection.commit()

    # Reload the plan_track_df
    st.session_state["plan_track_df"] = format_plan_track_table()
    connection.close()
    return f"{now_string} added to {tracked_sample}"


def overwrite_db_with_csv(uploaded_file, connection):
    if uploaded_file is not None:
        try:
            # 1. Read CSV to DataFrame
            df = pd.read_csv(uploaded_file)

            # 2. Overwrite plan_track table with the DataFrame
            df.to_sql("plan_track", connection, if_exists="replace", index=False)

            st.toast("CSV imported successfully and plan_track table overwritten.")
            # Reload the plan_track_df
            st.session_state["plan_track_df"] = format_plan_track_table()

        except Exception as e:
            st.toast(f"Failed to import CSV: {e}")


@st.fragment(run_every="1s")
def next_scan_countdown():
    # Get current time
    now = datetime.now(ZoneInfo("Europe/Berlin"))

    # Get all future planned scan times
    if "plan_track_df" in st.session_state:

        plan_track_df = st.session_state["plan_track_df"]
        future_scans = plan_track_df[plan_track_df["timestamp"] > now].sort_values("timestamp", ascending=True)
        next_sample = future_scans["sample"].values[0]
        next_scan_time = future_scans["timestamp"].iloc[0]

        next_sample_T = samples.samples[next_sample]["T"]
        next_sample_solution = samples.samples[next_sample]["solution"]
        next_sample_profile = samples.samples[next_sample]["profile"]

        if next_scan_time:
            time_diff = next_scan_time - now
            mins, secs = divmod(int(time_diff.total_seconds()), 60)

            st.write(f"#### Next Scan: {next_sample}")
            cols = st.columns([0.55, 0.45])

            cols[0].write(f"""
                - **Scheduled: {next_scan_time.strftime("%H:%M:%S (%A)")}**
                - **Countdown: `{mins:02d}:{secs:02d}` remaining**
            """)

            cols[1].info(f""" 
            - T: {next_sample_T}  
            - Solution: {next_sample_solution}  
            - Profile: {next_sample_profile}""")

            if (mins <= 5) and (mins >=4) and (secs%5 == 0):
                st.balloons()

        else:
            st.success("✅ No upcoming scans found.")


@st.fragment(run_every="1s")
def sample20_countdown():
    # Get current time
    now = datetime.now(ZoneInfo("Europe/Berlin"))

    # Get all future planned scan times
    if "long_sag_df" in st.session_state:

        total_sag_df = st.session_state["long_sag_df"]
        sample_20_df = total_sag_df[total_sag_df["sample"]=="sample20"]
        sample_20_planned_df = sample_20_df[sample_20_df["source"] == "planned"]

        if not sample_20_planned_df["timestamp"].isna().all():
            future_scans = sample_20_planned_df[sample_20_planned_df["timestamp"] > now].sort_values("timestamp", ascending=True)
            #next_sample = future_scans["sample"].values[0]
            #next_sample = future_scans["sample"].values[0]
            if not future_scans.empty:
                next_scan_time = future_scans["timestamp"].iloc[0]
            else:
                next_scan_time = None
        else:
            next_scan_time = None

        # next_sample_T = samples.samples[next_sample]["T"]
        # next_sample_solution = samples.samples[next_sample]["solution"]
        # next_sample_profile = samples.samples[next_sample]["profile"]

        if next_scan_time:
            time_diff = next_scan_time - now
            mins, secs = divmod(int(time_diff.total_seconds()), 60)

            st.error(f"#  {mins:02d}:{secs:02d}")

            # cols[1].info(f"""
            # - T: {next_sample_T}
            # - Solution: {next_sample_solution}
            # - Profile: {next_sample_profile}""")

            if (mins <= 5) and (mins >=4) and (secs%5 == 0):
                st.balloons()

        else:
            st.success("✅ No upcoming events found.")

def sample21_countdown():
    # Get current time
    now = datetime.now(ZoneInfo("Europe/Berlin"))

    # Get all future planned scan times
    if "long_sag_df" in st.session_state:

        total_sag_df = st.session_state["long_sag_df"]
        sample_21_df = total_sag_df[total_sag_df["sample"]=="sample21"]
        sample_21_planned_df = sample_21_df[sample_21_df["source"] == "planned"]
        if not sample_21_planned_df["timestamp"].isna().all():
            future_scans = sample_21_planned_df[sample_21_planned_df["timestamp"] > now].sort_values("timestamp", ascending=True)

            #next_sample = future_scans["sample"].values[0]
            if not future_scans.empty:
                next_scan_time = future_scans["timestamp"].iloc[0]
            else:
                next_scan_time = None
        else:
            next_scan_time = None
        # next_sample_T = samples.samples[next_sample]["T"]
        # next_sample_solution = samples.samples[next_sample]["solution"]
        # next_sample_profile = samples.samples[next_sample]["profile"]


        if next_scan_time:
            time_diff = next_scan_time - now
            mins, secs = divmod(int(time_diff.total_seconds()), 60)

            st.error(f"#  {mins:02d}:{secs:02d}")

            # cols[1].info(f"""
            # - T: {next_sample_T}
            # - Solution: {next_sample_solution}
            # - Profile: {next_sample_profile}""")

            if (mins <= 5) and (mins >=4) and (secs%5 == 0):
                st.balloons()

        else:
            st.success("✅ No upcoming events found.")



@st.dialog("Upload Backup")
def upload_backup():

    uploaded_csv = st.file_uploader("Upload backup csv", type="csv", key="file_uploader")
    if uploaded_csv and uploaded_csv.size>0:
        connection = establish_db_connection()
        overwrite_db_with_csv(uploaded_csv, connection)
        del st.session_state["file_uploader"]
        connection.close()
        st.rerun()


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
        st.toast(f"{planned_sample} is an invalid name. Must be one of:\n {planned_sample_list}")

    start_time = datetime.now(ZoneInfo("Europe/Berlin")) # Experiment starts now
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
        st.toast(f"Sample plan successfully written to worksheet {planned_sample}")
        time.sleep(5)
        st.rerun()
    else:
        st.toast(f"Data in worksheet {planned_sample} already exists. No data was written")


def add_scan_to_track_df(tracked_sample):

    spreadsheet = st.session_state["spreadsheet"]
    tracked_sample_list = ["sample1_track", "sample2_track", "sample3_track", "sample4_track", "sample5_track", "sample6_track", "sample7_track", "sample8_track", "sample9_track"]

    if tracked_sample not in tracked_sample_list:
        return f"{tracked_sample} is an invalid name. Must be one of:\n {tracked_sample_list}"

    # Get the current time
    now = datetime.now(ZoneInfo("Europe/Berlin"))
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

#@st.cache_data(ttl=60)
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

    if not long_plan_track_df.empty:
        # Split the sample_name into 'sample' and 'source' -> plan/track
        long_plan_track_df["source"] = long_plan_track_df["sample"].apply(
            lambda x: "planned" if "_plan" in x else "tracked"
        )
        long_plan_track_df["sample"] = long_plan_track_df["sample"].str.replace("_plan", "")
        long_plan_track_df["sample"] = long_plan_track_df["sample"].str.replace("_track", "")
        long_plan_track_df.sort_values(by="timestamp", inplace=True)

    return long_plan_track_df