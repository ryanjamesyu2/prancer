# Python script to load the HHS data set
import sys
from utils import load_data, preprocess_hhs, get_connection, fmt_hospital
import psycopg
from updateTables import update_hospitals_table
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

            # 1. ---Insert into locations---
            update_hospitals_table(cursor, data)

            # 2. ---Insert into hospital---
            # get all hospitals in database
            cursor.execute(
                "SELECT hospital_pk FROM hospital",
            )
            db_hospital_pks = [row[0] for row in cursor.fetchall()]
            # each hospital_pk should appear once
            hosp_df = data[['hospital_pk', 'hospital_name', 'address',
                            'longitude', 'latitude', 'fips_code', 'zip']].drop_duplicates(subset=['hospital_pk'])
            # INSERT new hospitals
            insert_hosp_df = hosp_df[~hosp_df['hospital_pk'].isin(db_hospital_pks)]
            hosp_rows = []
            for _, r in insert_hosp_df.iterrows():
                hospital_pk = r['hospital_pk']
                hospital_name = r['hospital_name']
                address = r['address']
                longitude = r['longitude']
                latitude = r['latitude']
                fips_code = r['fips_code']
                zipcode = r['zip']

                hosp_rows.append((hospital_pk, hospital_name, address,
                                  longitude, latitude, fips_code, zipcode))
            cursor.executemany(
                """
                INSERT INTO hospital (hospital_pk, hospital_name, address, longitude, latitude, fips_code, zipcode)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (hospital_pk) DO NOTHING
                """, hosp_rows
            )
            print(f"Inserted {len(hosp_rows)} rows into hospital.")
            # existing hospitals to update
            cursor.execute(
                "SELECT hospital_pk, hospital_name, address, longitude, latitude, fips_code, zipcode FROM hospital"
            )
            db_hospital = pd.DataFrame(cursor.fetchall(), columns= ('hospital_pk', 'hospital_name', 'address',
                            'longitude', 'latitude', 'fips_code', 'zip'))
            update_hosp_df = hosp_df[hosp_df['hospital_pk'].isin(db_hospital_pks)]
            db_hospital = db_hospital[db_hospital['hospital_pk'].isin(update_hosp_df['hospital_pk'])]
            db_hospital = db_hospital.sort_values('hospital_pk').reset_index(drop=True)
            update_hosp_df = update_hosp_df.sort_values('hospital_pk').reset_index(drop=True)
            db_hospital = db_hospital[update_hosp_df.columns]
            rows_different = (update_hosp_df != db_hospital).any(axis=1)
            update_hosp_df = update_hosp_df[rows_different]
            hosp_rows = []
            for _, r in update_hosp_df.iterrows():
                hospital_pk = r['hospital_pk']
                hospital_name = r['hospital_name']
                address = r['address']
                longitude = r['longitude']
                latitude = r['latitude']
                fips_code = r['fips_code']
                zipcode = r['zip']

                hosp_rows.append((hospital_name, address,
                                  longitude, latitude, fips_code, zipcode, hospital_pk))
            cursor.executemany(
                """
                UPDATE hospital
                SET hospital_name = %s, address = %s, longitude = %s, 
                latitude = %s, fips_code = %s, zipcode = %s
                WHERE hospital_pk = %s
                """, hosp_rows
            )
            print(f"Updated {len(hosp_rows)} rows in hospital.")
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

            # 3. ---Insert into weekly_logs---
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
