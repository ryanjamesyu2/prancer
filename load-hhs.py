# Python script to load the HHS data set
import sys
from utils import (
    load_data,
    preprocess_hhs,
    get_connection,
    fmt_hospital,
    createErrorLog)
from updateTables import update_hospitals_table, update_locations_table

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
    loaded = len(data)
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
            # 1. ---Insert and update locations table---
            loc_rows, skipped = update_locations_table(cursor, data)

            # 2. ---Insert and update hospital tables---
            hosp_insert, hosp_update = update_hospitals_table(
                cursor, data, is_quality_data=False
            )

            # 3. ---Insert into weekly_logs---
            # Build hospital metadata lookup
            cursor.execute(
                """
                SELECT h.hospital_pk, h.hospital_name, h.address,
                       l.city, l.state, l.zipcode
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
            bad_rows = []
            for _, r in data.iterrows():
                collection_week = r['collection_week']
                adult_beds_available_avg = r[
                    'all_adult_hospital_beds_7_day_avg'
                ]
                pediatric_beds_available_avg = r[
                    'all_pediatric_inpatient_beds_7_day_avg'
                ]
                adult_beds_occupied_avg = r[
                    'all_adult_hospital_inpatient_bed_occupied_7_day_avg'
                ]
                pediatric_beds_occupied_avg = r[
                    'all_pediatric_inpatient_bed_occupied_7_day_avg'
                ]
                icu_beds_available_avg = r['total_icu_beds_7_day_avg']
                icu_beds_occupied_avg = r['icu_beds_used_7_day_avg']
                confirmed_covid_hospitalized_avg = r[
                    'inpatient_beds_used_covid_7_day_avg'
                ]
                confirmed_covid_icu_avg = r[
                    'staffed_icu_adult_patients_confirmed_covid_7_day_avg'
                ]
                hospital_pk = r['hospital_pk']

                # ICU
                if (
                    icu_beds_available_avg is not None
                    and icu_beds_occupied_avg is not None
                    and icu_beds_occupied_avg > icu_beds_available_avg
                ):
                    bad_rows.append(
                        (f"[SKIP] ICU occupied > available for "
                         f"{fmt_hospital(hospital_pk, hospital_info)} "
                         f"({icu_beds_occupied_avg} > "
                         f"{icu_beds_available_avg})")
                    )
                    continue

                # Adult
                if (
                    adult_beds_available_avg is not None
                    and adult_beds_occupied_avg is not None
                    and adult_beds_occupied_avg > adult_beds_available_avg
                ):
                    bad_rows.append(
                        (f"[SKIP] Adult beds occupied > available for "
                         f"{fmt_hospital(hospital_pk, hospital_info)} "
                         f"({adult_beds_occupied_avg} > "
                         f"{adult_beds_available_avg})")
                    )
                    continue

                # Pediatric
                if (
                    pediatric_beds_available_avg is not None
                    and pediatric_beds_occupied_avg is not None
                    and pediatric_beds_occupied_avg
                        > pediatric_beds_available_avg
                ):
                    bad_rows.append(
                        (f"[SKIP] Pediatric beds occupied > available for "
                         f"{fmt_hospital(hospital_pk, hospital_info)} "
                         f"({pediatric_beds_occupied_avg} > "
                         f"{pediatric_beds_available_avg})")
                    )
                    continue

                # COVID ICU > COVID hospitalized
                if (
                    confirmed_covid_hospitalized_avg is not None
                    and confirmed_covid_icu_avg is not None
                    and confirmed_covid_icu_avg
                        > confirmed_covid_hospitalized_avg
                ):
                    bad_rows.append(
                        (f"[SKIP] COVID ICU > COVID hospitalized for "
                         f"{fmt_hospital(hospital_pk, hospital_info)} "
                         f"({confirmed_covid_icu_avg} > "
                         f"{confirmed_covid_hospitalized_avg})")
                    )
                    continue

                weekly_rows.append((
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
                ))
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

            errors = skipped + bad_rows
            createErrorLog(errors, "hhs")

            print("\nSummary:")
            print(f"Loaded {loaded} rows from the provided .CSV file.")
            print(f"Inserted {loc_rows} new rows into locations.")
            print(
                f"Skipped {len(skipped)} rows due to null city/state/zipcode."
            )
            print(f"Inserted {hosp_insert} rows into hospital.")
            print(f"Updated {hosp_update} rows in hospital.")
            print(f"Inserted {len(weekly_rows)} rows into weekly_logs.\n"
                  f"Skipped {len(bad_rows)} inconsistent rows.")

    except Exception as e:
        print("Error inserting data", e)
        raise

    finally:
        conn.close()
        cursor.close()


if __name__ == "__main__":
    main()
