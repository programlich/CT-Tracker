from logging import exception

import numpy as np
import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime, timedelta
from data_handling_functions import *
import gspread
from oauth2client.service_account import ServiceAccountCredentials

st.set_page_config(layout="wide")

# Authentication and conection to google docs -> store spreadsheet in the session state
# if "spreadsheet" not in st.session_state:
#     connect_to_docs()
if "connection" not in st.session_state:
    st.session_state["connection"] = establish_db_connection()
connection = st.session_state["connection"]

# Get the complete plan_df from docs and save it to session state. Only reload, if the docs have been changed
if "plan_track_df" not in st.session_state:
    st.session_state["plan_track_df"] = format_plan_track_table(connection)
    # try:
    #     st.session_state["plan_track_df"] = aggregate_plan_and_track_data()
    # except gspread.exceptions.APIError:
    #     st.toast("Quota limit reached. Wait for a minute")


plan_track_df = st.session_state["plan_track_df"]

if not plan_track_df.empty:
    started_samples = plan_track_df.loc[plan_track_df["source"] == "planned", "sample"].unique().tolist()
    tracked_samples = plan_track_df.loc[plan_track_df["source"] == "tracked", "sample"].unique().tolist()
else:
    started_samples = []

plot_container = st.container()
widget_container = st.container(border=True)
selected_sample = widget_container.pills("Sample", options=["sample1", "sample2", "sample3", "sample4", "sample5", "sample6",
                                                   "sample7", "sample8", "sample9"], selection_mode="single", )
widget_cols = widget_container.columns(3, vertical_alignment="bottom")

# Handle non initialized samples
if selected_sample not in started_samples and selected_sample:
    if widget_cols[1].button(f"Initialize experiment for {selected_sample} now", type="primary", use_container_width=True):
        try:
            add_plan_df_to_db(selected_sample, connection)
            # create_plan_df(f"{selected_sample}_plan")
        # except gspread.exceptions.APIError:
        except Exception as e:
            # st.toast("Quota limit reached. Wait for a minute")
            st.toast(e)

elif selected_sample in started_samples and selected_sample:
    # date = widget_cols[0].date_input("Date", value="2025-06-10", format="DD.MM.YYYY")
    # time = widget_cols[1].time_input("Time", step=60)

    # Add a given point of time to the track_df
    if widget_cols[1].button(f"Add scan to {selected_sample}", type="primary", use_container_width=True):
        try:
            response = add_scan_to_track_df(f"{selected_sample}_track")
            st.toast(response)
        except gspread.exceptions.APIError:
            st.toast("Quota limit reached. Wait for a minute")

# Get the current state of data from the session state
plan_track_df = st.session_state["plan_track_df"]

if not plan_track_df.empty:
    # Create the plot
    fig = px.scatter(plan_track_df, x="timestamp", y="sample", color="source", color_discrete_map={
    "planned": "#f39c12",  # orange
    "tracked": "#2ecc71"  # greenish
       },
                     symbol="source", )

    # Set the x axis over the whole day
    #start_time = datetime(2025,6,10,8,0)
    #end_time = datetime(2025,6,11,8,0)

    fig.update_layout(
         xaxis=dict(
    #        range=[start_time, end_time],
             title_text="Date and Time",
             tickmode="linear",
    # tick0=start_time,           # Start ticks from this point
            dtick=3600000,              # 1 hour in milliseconds (1000 ms * 60 s * 60 min)
         )
     )

    # Sort the y axis according to the number of the sample
    present_samples = (
        plan_track_df["sample"]
        .dropna()
        .unique()
        .tolist()
    )
    present_samples.sort(reverse=True)
    fig.update_yaxes(
        categoryorder="array",
        categoryarray=present_samples
    )

    # Add time indicator with red horizontal line
    now = datetime.now()
    fig.add_shape(
        type="line",
        x0=now,
        y0=0,
        x1=now,
        y1=1,
        xref="x",
        yref="paper",  # Full vertical height
        line=dict(
            color="red",
            width=2,
            dash="dash"
        )
    )

    # Beautify styling of the plot
    fig.update_traces(marker=dict(size=10))  # 🔍 Adjust marker size here
    fig.update_layout(
        xaxis=dict(
            showline=True,          # Show bottom + top axis line
            linecolor="#dcdcdc",    # Light grey line color
            linewidth=2,
            mirror=True,            # Mirror axis lines on top
            ticks='outside',        # Optional: show ticks outside
            tickfont=dict(color="#dcdcdc")
        ),
        yaxis=dict(
            showline=True,
            linecolor="#dcdcdc",
            linewidth=2,
            mirror=True,            # Mirror axis lines on right
            ticks='outside',
            tickfont=dict(color="#dcdcdc")
        ),
        plot_bgcolor="#1e1e1e",     # Match your dark background
        paper_bgcolor="#1e1e1e",
        font=dict(color="#dcdcdc"),

        # 🔧 Add extra margin to ensure right y-axis and top x-axis are visible
        margin=dict(l=60, r=60, t=60, b=60)
    )

    # Show the plot
    plot_container.plotly_chart(fig, use_container_width=True, theme="streamlit")


    @st.fragment(run_every="1s")
    def next_scan_countdown():
        # Get current time
        now = datetime.now()

        # Get all future planned scan times
        plan_df = st.session_state["plan_track_df"]
        future_scans = plan_df[plan_df["timestamp"] > now].sort_values("timestamp", ascending=True)
        next_sample = future_scans["sample"].values[0]
        next_scan_time = pd.to_datetime(future_scans["timestamp"].values[0])

        alarm_container = st.container(border=True)
        if next_scan_time:
            time_diff = next_scan_time - now
            mins, secs = divmod(int(time_diff.total_seconds()), 60)

            alarm_container.write(f"""
                #### Next Scan: {next_sample}
                - **Scheduled: {next_scan_time.strftime("%H:%M:%S (%A)")}**
                - **Countdown: `{mins:02d}:{secs:02d}` remaining**
            """)
        else:
            alarm_container.success("✅ No upcoming scans found.")


    next_scan_countdown()



else:
    plot_container.info("No experiment initialized yet.")


# Show current date and time above the plot
# cols[1].info(f"Date: {future_now.day}.{future_now.month}      Time: {future_now.hour}:{future_now.minute}")




