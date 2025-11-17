# Python script to load the HHS data set
import sys
from utils import load_data, preprocess_hhs, get_connection, fmt_hospital
import psycopg
from updateTables import update_hospitals_table, update_locations_table
import credentials
import pandas as pd

# Driver code to load data


# Load data from file path determined by first command line argument
cols = [
    'hospital_pk', 'state', 'hospital_name', 'address', 'city', 'zip',
    'fips_code', 'geocoded_hospital_address', 'collection_week',
    'all_adult_hospital_beds_7_day_avg',
    'all_pediatric_inpatient_beds_7_day_avg',
    'all_adult_hospital_inpatient_bed_occupied_7_day_avg',
    'all_pediatric_inpatient_bed_occupied_7_day_avg',
    'total_icu_beds_7_day_avg', 'icu_beds_used_7_day_avg',
    'inpatient_beds_used_covid_7_day_avg',
    'staffed_icu_adult_patients_confirmed_covid_7_day_avg'
]

try:
    data = load_data(sys.argv[1], cols)
except Exception as e:
    print("Error loading HHS data:", e)

try:
    data = preprocess_hhs(data)
except Exception as e:
    print("Error preprocessing HHS data:", e)

# data = preprocess_hhs(data)

# Use try-except to insert, with rollback in except to make sure no data
# is inserted if there's an error

# Driver code to update hospital table if necessary



def main():
    conn = get_connection()
    cursor = conn.cursor()

    try:
        with conn.transaction():

            # 1. ---Insert and update hospital tables---
            update_hospitals_table(cursor, data)

            # 2. ---Insert and update locations table---
            update_locations_table(cursor, data, is_quality_data = False)

            # 3. ---Insert into weekly_logs---
            # Build hospital metadata lookup
            cursor.execute(
                """
                SELECT h.hospital_pk, h.hospital_name, h.address, l.city, l.state, l.zipcode
                FROM hospital h
                JOIN locations l ON h.zipcode = l.zipcode
                """
            )
            hospital_info = {}
            for row in cursor.fetchall():
                pk, name, address, city, state, zipcode = row
                hospital_info[pk] = {
                    "name": name,
                    "address": address,
                    "city": city,
                    "state": state,
                    "zip": zipcode
                }
            weekly_rows = []
            bad_rows = 0
            for _, r in data.iterrows():
                collection_week = r['collection_week']
                adult_beds_available_avg = r['all_adult_hospital_beds_7_day_avg']
                pediatric_beds_available_avg = r['all_pediatric_inpatient_beds_7_day_avg']
                adult_beds_occupied_avg = r['all_adult_hospital_inpatient_bed_occupied_7_day_avg']
                pediatric_beds_occupied_avg = r['all_pediatric_inpatient_bed_occupied_7_day_avg']
                icu_beds_available_avg = r['total_icu_beds_7_day_avg']
                icu_beds_occupied_avg = r['icu_beds_used_7_day_avg']
                confirmed_covid_hospitalized_avg = r['inpatient_beds_used_covid_7_day_avg']
                confirmed_covid_icu_avg = r['staffed_icu_adult_patients_confirmed_covid_7_day_avg']
                hospital_pk = r['hospital_pk']

                # ICU
                if (icu_beds_available_avg is not None and
                    icu_beds_occupied_avg is not None and
                    icu_beds_occupied_avg > icu_beds_available_avg):

                    print(f"[SKIP] ICU occupied > available for {fmt_hospital(hospital_pk, hospital_info)} "
                          f"({icu_beds_occupied_avg} > {icu_beds_available_avg})")
                    bad_rows += 1
                    continue

                # Adult
                if (adult_beds_available_avg is not None and
                    adult_beds_occupied_avg is not None and
                    adult_beds_occupied_avg > adult_beds_available_avg):

                    print(f"[SKIP] Adult beds occupied > available for {fmt_hospital(hospital_pk, hospital_info)} "
                          f"({adult_beds_occupied_avg} > {adult_beds_available_avg})")
                    bad_rows += 1
                    continue

                # Pediatric
                if (pediatric_beds_available_avg is not None and
                    pediatric_beds_occupied_avg is not None and
                    pediatric_beds_occupied_avg > pediatric_beds_available_avg):

                    print(f"[SKIP] Pediatric beds occupied > available for {fmt_hospital(hospital_pk, hospital_info)} "
                          f"({pediatric_beds_occupied_avg} > {pediatric_beds_available_avg})")
                    bad_rows += 1
                    continue

                # COVID ICU > COVID hospitalized
                if (confirmed_covid_hospitalized_avg is not None and
                    confirmed_covid_icu_avg is not None and
                    confirmed_covid_icu_avg > confirmed_covid_hospitalized_avg):

                    print(f"[SKIP] COVID ICU > COVID hospitalized for {fmt_hospital(hospital_pk, hospital_info)} "
                          f"({confirmed_covid_icu_avg} > {confirmed_covid_hospitalized_avg})")
                    bad_rows += 1
                    continue

                weekly_rows.append((collection_week, adult_beds_available_avg, pediatric_beds_available_avg,
                                    adult_beds_occupied_avg, pediatric_beds_occupied_avg, icu_beds_available_avg,
                                    icu_beds_occupied_avg, confirmed_covid_hospitalized_avg,
                                    confirmed_covid_icu_avg, hospital_pk))
            cursor.executemany(
                """
                INSERT INTO weekly_logs (
                    collection_week,
                    adult_beds_available_avg,
                    pediatric_beds_available_avg,
                    adult_beds_occupied_avg,
                    pediatric_beds_occupied_avg,
                    icu_beds_available_avg,
                    icu_beds_occupied_avg,
                    confirmed_covid_hospitalized_avg,
                    confirmed_covid_icu_avg,
                    hospital_pk
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                """, weekly_rows
            )
            print(f"Inserted {len(weekly_rows)} rows into weekly_logs. Skipped {bad_rows} inconsistent rows.")

    except Exception as e:
        print("Error inserting data", e)
        raise

    finally:
        conn.close()
        cursor.close()


if __name__ == "__main__":
    main()
