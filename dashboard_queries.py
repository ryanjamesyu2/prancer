# -----------------
# Dashboard queries
# -----------------

get_weeks = """
SELECT DISTINCT collection_week AS week
FROM weekly_logs
ORDER BY week DESC;
"""

get_states = """
SELECT DISTINCT state
FROM locations
ORDER BY state;
"""

# 1
weekly_records_summary = """
SELECT collection_week as "Collection Week", COUNT(*) AS "Count of Records Loaded"
FROM weekly_logs
GROUP BY collection_week
ORDER BY collection_week;
"""

# 2
bed_summary_5_weeks = """
/*
Return current week + previous 4 weeks, with total beds and covid usage
*/
WITH last_weeks AS (
    SELECT DISTINCT collection_week AS week
    FROM weekly_logs
    WHERE collection_week <= %(week)s
    ORDER BY week DESC
    LIMIT 5
)
SELECT
    l.state,
    wl.collection_week AS week,
    SUM(wl.adult_beds_available_avg) AS adult_beds_available,
    SUM(wl.adult_beds_occupied_avg) AS adult_beds_used,
    SUM(wl.pediatric_beds_available_avg) AS pediatric_beds_available,
    SUM(wl.pediatric_beds_occupied_avg) AS pediatric_beds_used,
    SUM(wl.confirmed_covid_hospitalized_avg) AS covid_beds_used
FROM weekly_logs wl
JOIN last_weeks lw ON wl.collection_week = lw.week
JOIN hospital h ON wl.hospital_pk = h.hospital_pk
JOIN locations l ON h.zipcode = l.zipcode
GROUP BY l.state, wl.collection_week
ORDER BY l.state, wl.collection_week DESC
"""

# 3
beds_fraction_by_quality = """
WITH latest_quality AS (
    SELECT DISTINCT ON (hospital_pk)
        hospital_pk,
        quality_rating,
        date_updated
    FROM hospital_quality
    ORDER BY hospital_pk, date_updated DESC
),
hospital_fraction AS (
    SELECT
        l.state,
        wl.hospital_pk,
        wl.collection_week,
        lq.quality_rating,
        lq.date_updated,
        wl.adult_beds_occupied_avg
        / NULLIF(wl.adult_beds_available_avg, 0) AS adult_fraction_used,
        wl.pediatric_beds_occupied_avg
        / NULLIF(wl.pediatric_beds_available_avg, 0) AS pediatric_fraction_used,
        wl.icu_beds_occupied_avg
        / NULLIF(wl.icu_beds_available_avg, 0) AS icu_fraction_used,
        (wl.adult_beds_occupied_avg + wl.pediatric_beds_occupied_avg + wl.icu_beds_occupied_avg)
        / NULLIF(wl.adult_beds_available_avg + wl.pediatric_beds_available_avg + wl.icu_beds_available_avg, 0) AS fraction_used
    FROM weekly_logs wl
    JOIN hospital h ON wl.hospital_pk = h.hospital_pk
    JOIN locations l ON h.zipcode = l.zipcode
    JOIN latest_quality lq ON wl.hospital_pk = lq.hospital_pk
    WHERE wl.collection_week = %(week)s
)
SELECT
    quality_rating,
    AVG(adult_fraction_used) AS adult,
    AVG(pediatric_fraction_used) AS pediatric,
    AVG(icu_fraction_used) AS icu,
    AVG(fraction_used) AS total,
    COUNT(*) AS num_hospitals
FROM hospital_fraction
GROUP BY quality_rating
ORDER BY quality_rating
"""

# 4
beds_used_over_time = """
SELECT
    wl.collection_week,
    SUM(wl.adult_beds_occupied_avg + wl.pediatric_beds_occupied_avg + wl.icu_beds_occupied_avg) AS all,
    SUM(wl.confirmed_covid_hospitalized_avg) AS covid
FROM weekly_logs wl
JOIN hospital h ON wl.hospital_pk = h.hospital_pk
JOIN locations l ON h.zipcode = l.zipcode
WHERE wl.collection_week <= %(week)s
GROUP BY wl.collection_week
ORDER BY wl.collection_week
"""

# 5. A map of average hospital quality by state
avg_quality_by_state = """
WITH latest_quality AS (
    SELECT DISTINCT ON (hospital_pk)
        hospital_pk,
        quality_rating
    FROM hospital_quality
    ORDER BY hospital_pk, date_updated DESC
)
SELECT 
    l.state,
    AVG(
        CASE 
            WHEN lq.quality_rating = 'Not Available' THEN NULL
            ELSE CAST(CAST(lq.quality_rating AS TEXT) AS INTEGER)
        END
    ) AS avg_quality_rating,
    COUNT(*) FILTER (WHERE lq.quality_rating != 'Not Available') AS num_hospitals_rated,
    COUNT(*) AS total_hospitals
FROM hospital h
JOIN locations l ON l.zipcode = h.zipcode
JOIN latest_quality lq ON lq.hospital_pk = h.hospital_pk
GROUP BY l.state
ORDER BY avg_quality_rating DESC NULLS LAST
"""

# 6. Covid time series by ownership
covid_by_ownership = """
WITH latest_quality AS (
    SELECT DISTINCT ON (hospital_pk)
        hospital_pk, 
        type_of_ownership
    FROM hospital_quality
    ORDER BY hospital_pk, date_updated DESC
)
SELECT
    wl.collection_week,
    lq.type_of_ownership,
    SUM(wl.confirmed_covid_hospitalized_avg) AS covid_cases
FROM weekly_logs wl
JOIN latest_quality lq ON wl.hospital_pk = lq.hospital_pk
WHERE wl.collection_week <= %s
GROUP BY wl.collection_week, lq.type_of_ownership
ORDER BY wl.collection_week, lq.type_of_ownership
"""

# 7. Beds in used by emergency services
beds_by_emergency_services = """
WITH latest_quality AS (
    SELECT DISTINCT ON (hospital_pk)
        hospital_pk,
        emergency_services
    FROM hospital_quality
    ORDER BY hospital_pk, date_updated DESC
)
SELECT
    l.state,
    lq.emergency_services,
    SUM(wl.adult_beds_occupied_avg) AS adult_beds_in_use,
    SUM(wl.pediatric_beds_occupied_avg) AS pediatric_beds_in_use,
    SUM(wl.icu_beds_occupied_avg) AS icu_beds_in_use,
    SUM(wl.confirmed_covid_hospitalized_avg) As covid_beds_in_use
FROM weekly_logs wl
JOIN latest_quality lq ON wl.hospital_pk = lq.hospital_pk
JOIN hospital h ON wl.hospital_pk = h.hospital_pk
JOIN locations l ON h.zipcode = l.zipcode
WHERE wl.collection_week = %s
GROUP BY l.state, lq.emergency_services
ORDER BY l.state, lq.emergency_services DESC;
"""
