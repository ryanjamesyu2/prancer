import streamlit as st
import dashboard_queries as queries
import dashboard_utils as utils
import altair as alt
import pandas as pd


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


# ----------Plot/Table #2----------
st.subheader("Adult & Pediatric & COVID Beds: Current Week vs Previous 4 Weeks")
"""
A table summarizing the number of adult and pediatric beds available that week,
the number used, and the number used by patients with COVID,
compared to the 4 most recent weeks.
"""

beds_df = utils.run_query(
    queries.bed_summary_5_weeks,
    params={"week": selected_week}
)
st.dataframe(beds_df, use_container_width=True, hide_index=True)


# ----------Plot/Table #7: Beds in use by emergency services----------
st.subheader("Beds in Use by Emergency Services (Selected Week)")
"""
Number of adult, pediatric, ICU, and COVID beds in use in the selected week,
broken down by state and whether the hospital has emergency services.
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
