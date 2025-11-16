# Python script to load the hospital quality data set
import sys
from utils import load_data, preprocess_quality
import psycopg
from datetime import datetime
import credentials

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
        host=credentials.DB_HOST,
        dbname=credentials.DB_NAME,
        user=credentials.DB_USER,
        password=credentials.DB_PASSWORD)


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
            locations_set = set()  # (zipcode, state, city)
            hospital_info = {}     # hospital_pk -> (name, address, zipcode)
            skipped_missing_basic_info = 0

            quality_rows = []

            for _, r in data.iterrows():
                # Basic identifiers from quality csv
                hospital_pk = str(r["Facility ID"]).strip() if r["Facility ID"] is not None else ""
                name = str(r["Facility Name"]).strip() if r["Facility Name"] is not None else ""
                address = str(r["Address"]).strip() if r["Address"] is not None else None
                city = str(r["City"]).strip() if r["City"] is not None else ""
                state = str(r["State"]).strip() if r["State"] is not None else ""
                zipcode = r["ZIP Code"]
                # Normalize quality rating to ENUM
                raw_q = str(r['Hospital overall rating']).strip()
                quality_rating = raw_q if raw_q in {'1', '2', '3', '4', '5'} else "Not Available"
                hosp_type = r['Hospital Type']
                ownership = r['Hospital Ownership']
                emergency = parse_emergency(r["Emergency Services"])

                # If we don't have enough info to meaningfully link this hospital, skip it
                if not hospital_pk or not name or not city or not state or not zipcode:
                    skipped_missing_basic_info += 1
                    continue

                locations_set.add((zipcode, state, city))
                hospital_info[hospital_pk] = (name, address, zipcode)
                quality_rows.append((quality_rating, date_updated, hosp_type, ownership, emergency, hospital_pk))

            location_rows = list(locations_set)
            # Ensure a row in locations for this zipcode/state/city
            cursor.executemany(
                """
                INSERT INTO locations (zipcode, state, city)
                VALUES (%s, %s, %s)
                ON CONFLICT (zipcode) DO NOTHING
                """, location_rows,
            )
            new_locations = cursor.rowcount # number of actuallt inserted rows

            # Fetch existing hospitals
            cursor.execute("SELECT hospital_pk FROM hospital")
            existing_pks = {row[0] for row in cursor.fetchall()}

            # Insert/update hospitals
            new_hospitals_rows = []
            update_hospitals_rows = []
            for hospital_pk, (name, address, zipcode) in hospital_info.items():
                if hospital_pk in existing_pks:
                    # Existing hospital: update basic metadata that might change
                    update_hospitals_rows.append((name, address, zipcode, hospital_pk))
                else:
                    # New hospital: we only know info from quality data,
                    # so longitude/latitude/fips_code = NULL.
                    new_hospitals_rows.append((hospital_pk, name, address, None, None, None, zipcode))

            # Insert new hospitals
            new_hospitals = 0
            if new_hospitals_rows:
                cursor.executemany(
                    """
                    INSERT INTO hospital (hospital_pk, hospital_name, address, longitude, latitude, fips_code, zipcode)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """, new_hospitals_rows,
                )
                new_hospitals = cursor.rowcount

            # Update existing hospitals
            updated_hospitals = 0
            if update_hospitals_rows:
                cursor.executemany(
                    """
                    UPDATE hospital
                    SET hospital_name = %s, address = %s, zipcode = %s
                    WHERE hospital_pk = %s
                    """, update_hospitals_rows,
                )
                updated_hospitals = cursor.rowcount

            if quality_rows:
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
            print(f"New locations inserted: {new_locations}")
            print(f"New hospitals inserted: {new_hospitals}")
            print(f"Existing hospitals updated: {updated_hospitals}")
            print(f"Rows skipped due to missing basic info: {skipped_missing_basic_info}")

    except Exception as e:
        print("Error inserting data", e)
        raise

    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    main()
