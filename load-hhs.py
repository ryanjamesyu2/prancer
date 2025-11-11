# Python script to load the HHS data set
import sys
from utils import load_data, preprocess_hhs


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
data = load_data(sys.argv[1], cols)
data = preprocess_hhs(data)

# data = preprocess_hhs(data)

# Use try-except to insert, with rollback in except to make sure no data
# is inserted if there's an error

# Driver code to update hospital table if necessary
