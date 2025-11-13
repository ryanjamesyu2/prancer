# Python script to load the hospital quality data set
import sys
from utils import load_data, preprocess_quality

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

try:
    data = load_data(sys.argv[1], cols)
except Exception as e:
    print("Error loading quality data:", e)

try:
    data = preprocess_quality(data, sys.argv[1])
except Exception as e:
    print("Error preprocessing quality data:", e)

# Use try-except to insert, with rollback in except to make sure no data
# is inserted if there's an error

# Driver code to update hospital table if necessary
