# Python script to load the hospital quality data set
import sys
from utils import load_data

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
data = load_data(sys.argv[1], cols)
# No other preprocessing to do--no missing values, all columns are correct
# data types. Only thing is to possibly rename columns if we want to

# Use try-except to insert, with rollback in except to make sure no data
# is inserted if there's an error

# Driver code to update hospital table if necessary
