# Python script to load the hospital quality data set
import sys
from utils import load_data, preprocess_quality
import psycopg
import pandas as pd
import credentials
from datetime import datetime

# Driver code to load data

cols = [
    "Facility ID",
    "Facility Name",
    "Address",
    "City",
    "State",
    "ZIP Code",
    "County Name",
    "Hospital Type",
    "Hospital Ownership",
    "Emergency Services",
    "Hospital overall rating"
]

date_str = sys.argv[1]
csv_file = sys.argv[2]

try:
    data = load_data(csv_file, cols)
except Exception as e:
    print("Error loading quality data:", e)

try:
    data = preprocess_quality(data, csv_file)
except Exception as e:
    print("Error preprocessing quality data:", e)

try:
    date_updated = datetime.strptime(date_str, "%Y-%m-%d").date()
except ValueError:
    print("Error: date must be in format YYYY-MM-DD")

# Use try-except to insert, with rollback in except to make sure no data
# is inserted if there's an error

# Driver code to update hospital table if necessary


def get_connection():
    return psycopg.connect(
        host="debprodserver.postgres.database.azure.com",
        dbname=credentials.DB_USER, user=credentials.DB_USER, password=credentials.DB_PASSWORD)


def parse_emergency(value):
    if value is None:
        return None
    s = str(value).strip().lower()
    if s == "yes":
        return True
    elif s == "no":
        return False
    else:
        return None


def main():
    conn = get_connection()
    cursor = conn.cursor()

    try:
        with conn.transaction():
            quality_rows = []
            skipped_missing_hospital = 0
            # get all zipcodes
            cursor.execute(
                "SELECT zipcode FROM locations",
            )
            db_zipcodes = [row[0] for row in cursor.fetchall()]
            # 1. ---Insert into locations---
            # drop duplicate (zip,state,city) combos to avoid redundant inserts
            loc_df = data[['ZIP Code', 'State', 'City']].drop_duplicates()
            # remove zipcodes already in database
            loc_df = loc_df[~loc_df['ZIP Code'].isin(db_zipcodes)]
            loc_rows = []
            skipped = 0
            for _, r in loc_df.iterrows():
                zipcode = r['ZIP Code']
                state = r['State']
                city = r['City']

                if pd.isna(zipcode) or pd.isna(state) or pd.isna(city):
                    print(f"Skipped: zipcode={zipcode}, state={state}, city={city}  (missing value)")
                    skipped += 1
                    continue

                loc_rows.append((zipcode, state, city))
            cursor.executemany(
                """
                INSERT INTO locations (zipcode, state, city)
                VALUES (%s, %s, %s)
                ON CONFLICT (zipcode) DO NOTHING
                """, loc_rows
            )
            print(f"Inserted {len(loc_rows)} new rows into locations.")
            print(f"Skipped {skipped} rows due to null city/state/zipcode.")


            # get all hospitals in database
            cursor.execute(
                "SELECT hospital_pk FROM hospital",
            )
            db_hospital_pks = [row[0] for row in cursor.fetchall()]
            for _, r in data.iterrows():
                hospital_pk = str(r['Facility ID']).strip()
                if not hospital_pk:
                    continue

                # Check that this hospital exists in main `hospital` table
                cursor.execute(
                    "SELECT 1 FROM hospital WHERE hospital_pk = %s",
                    (hospital_pk,),
                )
                if cursor.fetchone() is None:
                    print(f"[SKIP] Hospital {hospital_pk} not found in hospital table")
                    skipped_missing_hospital += 1
                    continue

                # Normalize quality rating to ENUM
                raw_q = str(r['Hospital overall rating']).strip()
                quality_rating = raw_q if raw_q in {'1', '2', '3', '4', '5'} else "Not Available"
                hosp_type = r['Hospital Type']
                ownership = r['Hospital Ownership']
                emergency = parse_emergency(r["Emergency Services"])

                quality_rows.append((quality_rating, date_updated, hosp_type, ownership, emergency, hospital_pk))

            cursor.executemany(
                """
                INSERT INTO hospital_quality (
                    quality_rating,
                    date_updated,
                    type_of_hospital,
                    type_of_ownership,
                    emergency_services,
                    hospital_pk
                )
                VALUES (%s, %s, %s, %s, %s, %s);
                """, quality_rows,
            )
            print(f"Inserted {len(quality_rows)} rows into hospital_quality.")
            print(f"Skipped {skipped_missing_hospital} rows due to missing hospitals.")

    except Exception as e:
        print("Error inserting data", e)
        raise

    finally:
        conn.close()
        cursor.close()


if __name__ == "__main__":
    main()
