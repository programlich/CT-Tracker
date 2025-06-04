import numpy as np
import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from data_handling_functions import *
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from samples import samples
import streamlit_authenticator as stauth
import yaml
from yaml.loader import SafeLoader

st.set_page_config(layout="wide")

# Login functionality
with open('.streamlit/users.yaml') as file:
    config = yaml.load(file, Loader=SafeLoader)

authenticator = stauth.Authenticate(
    config['credentials'],
    config['cookie']['name'],
    config['cookie']['key'],
    config['cookie']['expiry_days'],
)

if st.session_state["authentication_status"] is None:
    try:
        authenticator.login()
    except Exception as e:
        st.error(e)
    st.stop()

# Get the complete plan_df from docs and save it to session state. Only reload, if the docs have been changed
if "plan_track_df" not in st.session_state:
    st.session_state["plan_track_df"] = format_plan_track_table()
plan_track_df = st.session_state["plan_track_df"]

if not plan_track_df.empty:
    started_samples = plan_track_df.loc[plan_track_df["source"] == "planned", "sample"].unique().tolist()
    tracked_samples = plan_track_df.loc[plan_track_df["source"] == "tracked", "sample"].unique().tolist()
else:
    started_samples = []

# Widgets
plot_container = st.container()
widget_cols = st.columns(2)
sample_container = widget_cols[0].container(border=True)
countdown_container = widget_cols[1].container(border=True)
selected_sample = sample_container.pills("Sample", options=["sample1", "sample2", "sample3", "sample4", "sample5", "sample6",
                                                   "sample7", "sample8", "sample9"], selection_mode="single", )
# widget_cols = sample_container.columns(1, vertical_alignment="bottom")

# Handle non initialized samples
if selected_sample not in started_samples and selected_sample:
    if sample_container.button(f"Initialize experiment for {selected_sample} now", type="primary", use_container_width=True):
        try:
            add_plan_df_to_db(selected_sample)
            # create_plan_df(f"{selected_sample}_plan")
        # except gspread.exceptions.APIError:
        except Exception as e:
            # st.toast("Quota limit reached. Wait for a minute")
            st.toast(e)

elif selected_sample in started_samples and selected_sample:
    # date = widget_cols[0].date_input("Date", value="2025-06-10", format="DD.MM.YYYY")
    # time = widget_cols[1].time_input("Time", step=60)

    # Add a given point of time to the track_df
    if sample_container.button(f"Add scan to {selected_sample}", type="primary", use_container_width=True):
        try:
            # response = add_scan_to_track_df(f"{selected_sample}_track")
            response = add_scan_to_db(f"{selected_sample}_track")
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
    now = datetime.now(ZoneInfo("Europe/Berlin"))
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

    with countdown_container:
        next_scan_countdown()

else:
    countdown_container.info("No experiment initialized yet.")

st.divider()

# Show sample info
tabs = st.tabs(["Samples", "Data"])
sample_container = tabs[0].container(border=False)
sample_cols = sample_container.columns(3)

for i, sample in enumerate(samples):

    # Sample 1
    sample_cols[i%3].container(border=True).write(f"""##### Sample {i+1}
    - T: {samples[sample]["T"]}  
    - Solution: {samples[sample]['solution']}  
    - Profile: {samples[sample]['profile']}""")

# Show all data as dataframe
unformatted_plan_track_df = get_plan_track_table()
if "id" in unformatted_plan_track_df.columns:
    unformatted_plan_track_df.drop(columns=["id"], inplace=True)
tabs[1].container().dataframe(unformatted_plan_track_df, hide_index=True)

# Options to download/upload/delete data
data_actions_expander = st.expander("Data actions")
data_action_cols = data_actions_expander.columns(5)
csv_data = get_plan_track_table().to_csv(index=False)
data_action_cols[0].download_button("Download Backup",
                                    file_name=f"scans_{datetime.now(ZoneInfo("Europe/Berlin")).strftime("%Y-%m-%d_%H-%M-%S")}.csv",
                                    data=csv_data, use_container_width=True)


if data_action_cols[1].button("Upload Backup", use_container_width=True):
    upload_backup()

if data_action_cols[4].button("Delete All Data", use_container_width=True):
    delete_dialog()
