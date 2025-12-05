import streamlit as st
import dashboard_queries as queries
import dashboard_utils as utils
import altair as alt
import plotly.express as px


st.title("HHS Hospital Capacity Weekly Report")

# ---- Sidebar filters ----
st.sidebar.header("Filters")
available_weeks = utils.run_query(queries.get_weeks, params=())
weeks = sorted(available_weeks["week"].tolist(), reverse=True)
default_index = 0  # most recent week
selected_week = st.sidebar.selectbox(
    "Week (collection_week)",
    options=weeks,
    index=default_index,
)
st.caption(f"Report week: {selected_week}")

st.header("QUERY RESULTS")
# ----------Plot/Table #1: Totals of Weekly Logs----------
st.subheader("Number of Weekly Logs from Each Week")
"""
A table with counts for weekly logs from each week.
This shows records retrieved for user’s query
with comparison to previous weeks.
Note that the rows are listed from latest to earliest.
"""

weekly_counts = utils.run_query(
    queries.weekly_records_summary,
    params={"week": selected_week}
)

st.dataframe(weekly_counts, use_container_width=True, hide_index=True)

st.header("DATA SUMMARY")
# ----------Plot/Table #2: Adult & Pediatric & COVID Beds----------
st.subheader("Adult & Pediatric & COVID Beds: Current vs Previous 4 Weeks")
"""
A table summarizing the number of adult and pediatric beds available that week,
the number used, and the number used by patients with COVID,
compared to the 4 most recent weeks for each state.
Note that you can pick which week using the filter in the sidebar.
"""

beds_df = utils.run_query(
    queries.bed_summary_5_weeks,
    params={"week": selected_week}
)
st.dataframe(beds_df, use_container_width=True, hide_index=True)


# ----------Plot/Table #3: Time series of COVID cases----------
st.subheader("COVID Cases by Type of Hospital Ownership Per Week Over Time")
"""
Total COVID cases per each type of hospital ownership per week
Note that you can pick which week using the filter in the sidebar.
"""
covid_over_time = utils.run_query(
    queries.covid_by_ownership,
    params=(selected_week,)
)

st.dataframe(covid_over_time, use_container_width=True, hide_index=True)

st.line_chart(covid_over_time, x="collection_week", y="covid_cases",
              color="type_of_ownership", x_label="Week",
              y_label="Number of hospitalized patients with confirmed COVID",
              )

st.header("DATA VISUALIZATIONS")
# ----------Plot/Table #4: Beds in use by Quality----------
st.subheader("Proportion of Beds in Use by Hospital Quality")
"""
Proportion of total beds in use, broken down by hospital quality and bed type.
Note that you can pick which week using the filter in the sidebar.
"""
beds_by_quality = utils.run_query(
    queries.beds_fraction_by_quality,
    params={"week": selected_week}
)

plot3_df = beds_by_quality.melt(
    id_vars="quality_rating",
    value_vars=["adult",
                "pediatric",
                "icu",
                "total"],
    var_name="bed_type",
    value_name="avg_fraction_used",
)

chart3 = alt.Chart(plot3_df).mark_bar().encode(
    x=alt.X('quality_rating', title="Hospital Quality Rating"),
    xOffset="bed_type",  # ensures bars appear side by side
    y=alt.Y('avg_fraction_used', title="Fraction of Beds Used"),
    color=alt.Color('bed_type', title="Bed Type")
)

st.altair_chart(chart3)


# ----------Plot/Table #5: Beds in use over time----------
st.subheader("Total Hospital Beds Used Per Week Over Time")
"""
The total number of beds used per week up to the selected week, along side
the number of beds used for COVID patients.
Note that you can pick which week using the filter in the sidebar.
"""
beds_over_time = utils.run_query(
    queries.beds_used_over_time,
    params={"week": selected_week}
)

plot4_df = beds_over_time.melt(
    id_vars="collection_week",
    value_vars=["all",
                "covid"],
    var_name="Bed Type",
    value_name="beds_used",
)

st.line_chart(plot4_df, x="collection_week", y="beds_used", color="Bed Type",
              x_label="Week", y_label="Number of Beds Used",
              )


# ----------Plot/Table #6: Map of Hospital Quality----------
st.subheader("Hospital Quality Ratings Across the US")
"""
A map showing one dot for a hospital, colored by their latest quality rating.
"""
state_quality = utils.run_query(
    queries.avg_quality_by_state,
    {}
)

# st.map(plot5_df, latitude="latitude", longitude="longitude", color="color")
plot5 = px.choropleth(
    state_quality,
    locations='state',
    locationmode="USA-states",
    color='avg_quality_rating',
    scope="usa",
    color_continuous_scale="Viridis"
)

plot5.update_layout(coloraxis_colorbar=dict(
    title="Average Quality Rating"
))

st.plotly_chart(plot5)


# ----------Plot/Table #7: Beds in use by emergency services----------
st.subheader("Beds in Use by Emergency Services (Selected Week)")
"""
Number of adult, pediatric, ICU, and COVID beds in use in the selected week,
broken down by state and whether the hospital has emergency services.
Note that you can pick which week using the filter in the sidebar.
"""

beds_es_df = utils.run_query(
    queries.beds_by_emergency_services,
    params=(selected_week,)
)
beds_es_df["emergency_group"] = beds_es_df["emergency_services"].replace({
    True: "Yes",
    False: "No"
})
beds_es_df = beds_es_df.drop(columns=["emergency_services"])
st.dataframe(beds_es_df, use_container_width=True, hide_index=True)

agg = (beds_es_df.groupby("emergency_group", as_index=False)[[
    "adult_beds_in_use", "pediatric_beds_in_use", "icu_beds_in_use",]].sum()
)

# Long format for Altair
plot_df = agg.melt(
    id_vars="emergency_group",
    value_vars=["adult_beds_in_use",
                "pediatric_beds_in_use",
                "icu_beds_in_use",],
    var_name="bed_type",
    value_name="beds_in_use",
)
plot_df["bed_type"] = plot_df["bed_type"].map({
    "adult_beds_in_use": "Adult",
    "pediatric_beds_in_use": "Pediatric",
    "icu_beds_in_use": "ICU",
})

# Grouped bar chart
st.markdown("### Beds in Use by Emergency Services (National Totals)")
chart = (
    alt.Chart(plot_df)
    .mark_bar()
    .encode(
        x=alt.X("emergency_group:N", title="Emergency Services"),
        xOffset="bed_type:N",  # ensures bars appear side by side
        y=alt.Y("beds_in_use:Q", title="Beds in Use"),
        color=alt.Color("bed_type:N", title="Bed Type"),
        tooltip=[
            "emergency_group:N",
            "bed_type:N",
            alt.Tooltip("beds_in_use:Q", title="Beds in Use", format=","),
        ],
    )
    .properties(height=350)
)
st.altair_chart(chart, use_container_width=True)
