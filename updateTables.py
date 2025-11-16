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