import pandas as pd
import psycopg


def update_hospitals_table(cursor, data):
    # get all zipcodes
    cursor.execute(
        "SELECT zipcode FROM locations",
    )
    db_zipcodes = [row[0] for row in cursor.fetchall()]
            
    # drop duplicate (zip,state,city) combos to avoid redundant inserts
    loc_df = data[['zip', 'state', 'city']].drop_duplicates()
    # remove zipcodes already in database
    loc_df = loc_df[~loc_df['zip'].isin(db_zipcodes)]
    loc_rows = []
    skipped = 0
    for _, r in loc_df.iterrows():
        zipcode = r['zip']
        state = r['state']
        city = r['city']

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


def update_locations_table(cursor, data, is_quality_data):
    # get all hospitals in database
    cursor.execute(
        "SELECT hospital_pk FROM hospital",
    )
    db_hospital_pks = [row[0] for row in cursor.fetchall()]
    # each hospital_pk should appear once
    if (is_quality_data):
        hosp_df = data[['hospital_pk', 'hospital_name', 'address', 'zip']].drop_duplicates(subset=['hospital_pk'])
    else:
        hosp_df = data[['hospital_pk', 'hospital_name', 'address',
                        'longitude', 'latitude', 'fips_code', 'zip']].drop_duplicates(subset=['hospital_pk'])
    # INSERT new hospitals
    insert_hosp_df = hosp_df[~hosp_df['hospital_pk'].isin(db_hospital_pks)]
    hosp_rows = []
    for _, r in insert_hosp_df.iterrows():
        hospital_pk = r['hospital_pk']
        hospital_name = r['hospital_name']
        address = r['address']
        zipcode = r['zip']

        if (is_quality_data):
            hosp_rows.append((hospital_pk, hospital_name, address, zipcode))
        else:
            longitude = r['longitude']
            latitude = r['latitude']
            fips_code = r['fips_code']
            hosp_rows.append((hospital_pk, hospital_name, address,
                            longitude, latitude, fips_code, zipcode))
            
    if (is_quality_data):
        cursor.executemany(
            """
            INSERT INTO hospital (hospital_pk, hospital_name, address, longitude, latitude, fips_code, zipcode)
            VALUES (%s, %s, %s, NULL, NULL, NULL, %s)
            ON CONFLICT (hospital_pk) DO NOTHING
            """, hosp_rows
            )
    else:
        cursor.executemany(
            """
            INSERT INTO hospital (hospital_pk, hospital_name, address, longitude, latitude, fips_code, zipcode)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (hospital_pk) DO NOTHING
            """, hosp_rows
            )
    
    print(f"Inserted {len(hosp_rows)} rows into hospital.")
    # existing hospitals to update
    if (is_quality_data):
        cursor.execute(
            "SELECT hospital_pk, hospital_name, address, zipcode FROM hospital"
        )
        db_hospital = pd.DataFrame(cursor.fetchall(), columns= ('hospital_pk', 'hospital_name', 'address', 'zip'))
    else:
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
        zipcode = r['zip']
        if (is_quality_data):
            hosp_rows.append((hospital_name, address, zipcode, hospital_pk))
        else:
            longitude = r['longitude']
            latitude = r['latitude']
            fips_code = r['fips_code']
            hosp_rows.append((hospital_name, address,
                              longitude, latitude, fips_code, zipcode, hospital_pk))
    if (is_quality_data):
        cursor.executemany(
            """
            UPDATE hospital
            SET hospital_name = %s, address = %s, zipcode = %s
            WHERE hospital_pk = %s
            """, hosp_rows
        )
    else:
        cursor.executemany(
            """
            UPDATE hospital
            SET hospital_name = %s, address = %s, longitude = %s, 
            latitude = %s, fips_code = %s, zipcode = %s
            WHERE hospital_pk = %s
            """, hosp_rows
        )

    print(f"Updated {len(hosp_rows)} rows in hospital.")
