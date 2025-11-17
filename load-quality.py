# Python script to load the hospital quality data set
import sys
from utils import (
    load_data,
    preprocess_quality,
    get_connection,
    parse_emergency)
from updateTables import update_hospitals_table, update_locations_table
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
    loaded = len(data)
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


def main():
    conn = get_connection()
    cursor = conn.cursor()

    try:
        with conn.transaction():
            # 1. ---Insert and update locations table---
            loc_rows, skipped = update_locations_table(cursor, data)

            # 2. ---Insert and update hospital tables---
            hosp_insert, hosp_update = update_hospitals_table(
                cursor, data, is_quality_data=True
            )

            # 3. ---Insert into hospital_quality---
            quality_rows = []
            skipped_missing_hospital = 0
            for _, r in data.iterrows():
                # Normalize quality rating to ENUM
                raw_q = str(r['Hospital overall rating']).strip()
                valid_ratings = {'1', '2', '3', '4', '5'}
                quality_rating = (
                    raw_q if raw_q in valid_ratings else "Not Available"
                )

                hosp_type = r['Hospital Type']
                ownership = r['Hospital Ownership']
                emergency = parse_emergency(r["Emergency Services"])
                hospital_pk = r['hospital_pk']

                quality_rows.append((quality_rating, date_updated, hosp_type,
                                     ownership, emergency, hospital_pk))

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
            print("\nSummary:")
            print(f"Loaded {loaded} rows from the provided .CSV file.")
            print(f"Inserted {loc_rows} new rows into locations.")
            print(f"Skipped {skipped} rows due to null city/state/zipcode.")
            print(f"Inserted {hosp_insert} rows into hospital.")
            print(f"Updated {hosp_update} rows in hospital.")
            print(f"Inserted {len(quality_rows)} rows into hospital_quality.\n"
                  )
            print(
                f"Skipped {skipped_missing_hospital} "
                "rows due to missing hospitals."
            )

    except Exception as e:
        print("Error inserting data", e)
        raise

    finally:
        conn.close()
        cursor.close()


if __name__ == "__main__":
    main()
