# A python module to hold function definitions to aid in data loading
import pandas as pd
import numpy as np
from datetime import datetime


def load_data(filepath, cols):
    """A function to load a data file

    Parameters
    ----------
    filepath : str
        A string containing the file path to the data file to be loaded

    Returns
    -------
    Pandas DataFrame
        A data frame containing only the columns of interest from the HHS data
    """
    df = pd.read_csv(filepath)

    return df[cols]


def preprocess_hhs(data):
    """A function to pre-process a Pandas dataframe containing HHS data

    Parameters
    ----------
    data : DataFrame
        A Pandas DataFrame containing the columns of interest for the HHS data

    Returns
    -------
    DataFrame
        A Pandas DataFrame of processed data
    """

    # Convert columns to correct data type
    str_cols = [
        'hospital_pk', 'state', 'hospital_name', 'address', 'city', 'zip',
        'fips_code', 'geocoded_hospital_address',
    ]
    float_cols = [
        'all_adult_hospital_beds_7_day_avg',
        'all_pediatric_inpatient_beds_7_day_avg',
        'all_adult_hospital_inpatient_bed_occupied_7_day_avg',
        'all_pediatric_inpatient_bed_occupied_7_day_avg',
        'total_icu_beds_7_day_avg',
        'icu_beds_used_7_day_avg',
        'inpatient_beds_used_covid_7_day_avg',
        'staffed_icu_adult_patients_confirmed_covid_7_day_avg'
    ]
    data[str_cols] = data[str_cols].astype(str)
    data[float_cols] = data[float_cols].astype(float)
    data['collection_week'] = pd.to_datetime(data['collection_week'],
                                             format='%Y-%m-%d')

    # Replace missing values and -999999 with None
    data = data.replace(['nan', np.nan, -999999], None)

    # Split geocoded address to latitude and longitude
    geo_loc = data['geocoded_hospital_address'].str.split(' ')

    # trim extra parentheses, add to df, and drop geo location column
    lats = [row[1][1:] if row is not None else None for row in geo_loc]
    lons = [row[2][:-1] if row is not None else None for row in geo_loc]
    data['latitude'] = lats
    data['longitude'] = lons
    data = data.drop(columns=['geocoded_hospital_address'])

    # possible change column names if we want to do that

    return data


def add_date(data, filepath):
    """A function to add the date updated column to the Quality data

    Parameters
    ----------
    data : DataFrame
        A pandas data frame containing the information loaded from a
        quality .csv file
    filepath : str
        A string containing the filepath provided to load the data

    Returns
    -------
    DataFrame
        A pandas data frame containing the same information as the input
        with one extra column containing the date
    """

    # file name will be what is after the last /
    filename = filepath.split('/')[-1]
    parts = filename.split('-')

    # Assume each happens on the 15th (approximate middle) of each month
    date_updated = parts[1] + '-' + parts[2][:2] + "-15"
    data['date_updated'] = datetime.strptime(date_updated, '%Y-%m-%d')

    return data
