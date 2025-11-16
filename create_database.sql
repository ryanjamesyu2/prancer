CREATE TABLE locations (
    zipcode CHAR(5) PRIMARY KEY,
    state TEXT NOT NULL,
    city TEXT NOT NULL
);

CREATE TABLE hospital (
    hospital_pk TEXT PRIMARY KEY,
    hospital_name TEXT NOT NULL,
    address TEXT, 
    longitude FLOAT8,
    latitude FLOAT8,
    fips_code CHAR(5),
    zipcode CHAR(5) NOT NULL REFERENCES locations(zipcode)
);

CREATE TABLE weekly_logs (
    id SERIAL PRIMARY KEY,
    collection_week DATE NOT NULL,
    adult_beds_available_avg FLOAT8 CHECK (adult_beds_available_avg >=0),
    pediatric_beds_available_avg FLOAT8 CHECK (pediatric_beds_available_avg >=0),
    adult_beds_occupied_avg FLOAT8 CHECK (adult_beds_occupied_avg >=0),
    pediatric_beds_occupied_avg FLOAT8 CHECK (pediatric_beds_occupied_avg >=0),
    icu_beds_available_avg FLOAT8 CHECK (icu_beds_available_avg >=0),
    icu_beds_occupied_avg FLOAT8 CHECK (icu_beds_occupied_avg >=0),
    confirmed_covid_hospitalized_avg FLOAT8 CHECK (confirmed_covid_hospitalized_avg >=0),
    confirmed_covid_icu_avg FLOAT8 CHECK (confirmed_covid_icu_avg >=0),
    hospital_pk TEXT NOT NULL REFERENCES hospital(hospital_pk),
    CHECK(adult_beds_occupied_avg <= adult_beds_available_avg),
    CHECK(pediatric_beds_occupied_avg <= pediatric_beds_available_avg),
    CHECK(icu_beds_occupied_avg <= icu_beds_available_avg),
    CHECK(confirmed_covid_icu_avg <= confirmed_covid_hospitalized_avg)
);

CREATE TYPE quality AS ENUM ('1', '2', '3', '4', '5', 'Not Available');

CREATE TABLE hospital_quality (
    id SERIAL PRIMARY KEY,
    quality_rating quality NOT NULL,
    date_updated DATE NOT NULL,
    type_of_hospital TEXT,
    type_of_ownership TEXT,
    emergency_services BOOLEAN,
    hospital_pk TEXT NOT NULL REFERENCES hospital(hospital_pk)
);