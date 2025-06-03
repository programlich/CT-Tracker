import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta

st.set_page_config(layout="wide")

cols = st.columns([0.22, 0.78])

widget_container = cols[0].container(border=True)
sample = widget_container.pills("Sample", options=["Sample1", "Sample2", "Sample3"], selection_mode="single")
date = widget_container.date_input("Date", value="2025-06-10", format="DD.MM.YYYY")
time = widget_container.time_input("Time", step=60)

# Initialize and save the track_df
if "track_df" not in st.session_state:
    # Generate one datetime per minute over 24h, starting from a reference date
    start_time = datetime(year=2025, month=6, day=10, hour=8, minute=0)
    datetimes = pd.date_range(start=start_time, periods=24*60, freq='min')  # 'T' = minute frequency

    # Create DataFrame
    df = pd.DataFrame({'datetime': datetimes})

    df[["Sample1", "Sample2", "Sample3"]] = False

    st.session_state["track_df"] = df

# Initialize the plan_df
if "plan_df" not in st.session_state:
    # Generate one datetime per minute over 24h, starting from a reference date
    start_time = datetime(year=2025, month=6, day=10, hour=8, minute=0)
    datetimes = pd.date_range(start=start_time, periods=24*60, freq='min')  # 'T' = minute frequency

    # Create DataFrame
    df = pd.DataFrame({'datetime': datetimes})

    df[["Sample1_plan", "Sample2_plan", "Sample3_plan"]] = False

    df.loc[(df["datetime"].dt.day  == 10) & (df["datetime"].dt.hour  == 8) & (df["datetime"].dt.minute % 10 == 0), "Sample1_plan"] = True
    df.loc[(df["datetime"].dt.hour > 9) & (df["datetime"].dt.minute == 0), "Sample1_plan"] = True
    df.loc[(df["datetime"].dt.day == 11) & (df["datetime"].dt.minute == 0), "Sample1_plan"] = True

    df.loc[(df["datetime"].dt.day  == 10) & (df["datetime"].dt.hour  == 9) & (df["datetime"].dt.minute % 10 == 0), "Sample2_plan"] = True
    df.loc[(df["datetime"].dt.hour > 10) & (df["datetime"].dt.minute == 0), "Sample2_plan"] = True
    df.loc[(df["datetime"].dt.day == 11) & (df["datetime"].dt.minute == 0), "Sample2_plan"] = True

    df.loc[(df["datetime"].dt.day  == 10) & (df["datetime"].dt.hour  == 10) & (df["datetime"].dt.minute % 10 == 0), "Sample3_plan"] = True
    df.loc[(df["datetime"].dt.hour > 11) & (df["datetime"].dt.minute == 0), "Sample3_plan"] = True
    df.loc[(df["datetime"].dt.day == 11) & (df["datetime"].dt.minute == 0), "Sample3_plan"] = True

    st.session_state["plan_df"] = df


# Add a given point of time to the track_df
if widget_container.button(f"Add scan to {sample}", use_container_width=True):
    st.session_state["track_df"].loc[(st.session_state["track_df"]["datetime"].dt.day == date.day) &
                                     (st.session_state["track_df"]["datetime"].dt.hour == time.hour) &
                                     (st.session_state["track_df"]["datetime"].dt.minute == time.minute),
    sample] = True


track_plan_df = pd.merge(st.session_state["track_df"], st.session_state["plan_df"], on="datetime", how="inner")

# Melt track_df into long format
long_track_df = track_plan_df.melt(id_vars="datetime", value_vars=["Sample1", "Sample2", "Sample3",
                                                                   "Sample1_plan", "Sample2_plan", "Sample3_plan"],
                                                  value_name="scan", var_name="sample")
# Split 'full_sample' into 'sample' and 'source'
long_track_df["source"] = long_track_df["sample"].apply(
    lambda x: "planned" if "_plan" in x else "tracked"
)
long_track_df["sample"] = long_track_df["sample"].str.replace("_plan", "")

# Filter only rows where a scan is scheduled
long_track_df = long_track_df[long_track_df['scan']]


# Create the plot
fig = px.scatter(long_track_df, x="datetime", y="sample", color="source", color_discrete_map={
        "planned": "#f39c12",   # orange
        "tracked": "#2ecc71"    # greenish
    },
    symbol="source",)

# Set the x axis over the whole day
start_time = datetime(2025,6,10,8,0)
end_time = datetime(2025,6,11,8,0)

fig.update_layout(
    xaxis=dict(
        range=[start_time, end_time],
       # tickformat="%H:%M",  # Optional: Format x-axis ticks as time
        title_text="Date and Time",
        tickmode="linear",
tick0=start_time,           # Start ticks from this point
        dtick=3600000,              # 1 hour in milliseconds (1000 ms * 60 s * 60 min)
    )
)
fig.update_yaxes(
    categoryorder="array",
    categoryarray=["Sample3", "Sample2", "Sample1"]
)
# Add time indicator with red horizontal line
now = datetime.now()
future_now = datetime(2025,6,10,now.hour, now.minute)

fig.add_shape(
    type="line",
    x0=future_now,
    y0=0,
    x1=future_now,
    y1=1,
    xref="x",
    yref="paper",  # Full vertical height
    line=dict(
        color="red",
        width=2,
        dash="dash"
    )
)

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



# Show current date and time above the plot
cols[1].info(f"Date: {future_now.day}.{future_now.month}      Time: {future_now.hour}:{future_now.minute}")


# Show the plot
cols[1].plotly_chart(fig, use_container_width=True, theme="streamlit")

@st.fragment(run_every="1s")
def next_scan_countdown():
    # Get current time
    now = datetime.now()

    # Get all future planned scan times
    plan_df = st.session_state["plan_df"]
    future_scans = plan_df[plan_df["datetime"] > now]

    # Find the first True in any sample column
    next_scan = None
    for idx, row in future_scans.iterrows():
        for sample in ["Sample1_plan", "Sample2_plan", "Sample3_plan"]:
            if row[sample]:
                next_scan = {
                    "datetime": row["datetime"],
                    "sample": sample.replace("_plan", ""),
                }
                break
        if next_scan:
            break

    alarm_container = st.container(border=True)
    if next_scan:
        time_diff = next_scan["datetime"] - now
        mins, secs = divmod(int(time_diff.total_seconds()), 60)

        alarm_container.write(f"""
            **Upcoming Scan Alert**
            - **Sample:** `{next_scan['sample']}`
            - **Scheduled at:** `{next_scan['datetime'].strftime("%H:%M:%S")}`
            - **Countdown:** `{mins:02d}:{secs:02d} remaining`
        """)
    else:
        alarm_container.success("✅ No upcoming scans found.")

with cols[0]:
    next_scan_countdown()


with st.expander("Edit Data"):
    edited_df = st.data_editor(
        st.session_state["track_df"],
        use_container_width=True,
        disabled=["datetime"],
        key="edited_df_key",
        hide_index=True,
        num_rows = "fixed"
    )

    if not edited_df.equals(st.session_state["track_df"]):
        update = st.button("Update", type="primary")
        if update:
            st.session_state["track_df"] = edited_df
            st.rerun()