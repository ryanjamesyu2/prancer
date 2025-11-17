# Project Prancer
An ETL and reporting pipeline for HHS and Quality hospital data. The pipeline can be broadly broken down into 3 parts: schema, loading, and reporting. These parts are detailed in later sections. To run the code, use the `prancer_env` virtual environment provided in the `env.yml` file. You can create the environment by running the following command:

```
conda env create -f env.yml
```

## Part 1 - Schema

The database schema can be found in the `create_database.sql` file. We utilize 4 tables to store all relevant information about hospitals:

* **Hospital** - stores all descriptive information and metadata about hospitals that we believe will not change very frequently, if at all. Each record corresponds to one hospital.
* **Weekly Logs** - stores the weekly capacity and use of hospitals. Each row represents one week at one hospital.
* **Hospital Quality** - stores the overall hospital quality score assigned to hospitals at various dates. Each row is one quality rating for one hospital.
* **Location** - stores geographic information used to identify and locate hospitals. Each row is one ZIP code.

To create the database, connect to your desired database server in a python script or Jupyter notebook, and then sequentially run the code cells in `create_database.sql` script. The rationale behind our schema design decisions can be found in `project_part_one.ipynb`, though you will not need to run the code cells in the notebook.

## Part 2 - Loading Data

There are two main python scripts used to load information from local .CSV files into the database tables. These are `load-hhs.py` and `load-quality.py`. These scripts utilize helper functions defined in `utils.py`. Both main loading scripts require a local `credentials.py` module containing login information to the database you are trying to access. This is not provided in the repository, and each person needs to make their own version of the module with their own personal login information. The `credentials.py` file should look like the following, removing the brackets and replacing with your own information:

```
DB_USER = "[username]"
DB_PASSWORD = "[password]"
```

### Loading HHS Data

To load the HHS data into the database, run the script with the following terminal command:

```
python load-hhs.py [filepath]
```

The script takes one command line argument, describing where the data is stored on your local machine. The script assumes the data file itself follows the naming convention `YYYY-MM-DD-hhs-data.csv` (for example, `2022-09-23-hhs-data.csv`). Providing a .CSV file with a different naming convention could lead to errors loading the data.

The script first loads the data from the provided .CSV file, and then preprocesses the data. This includes converting data columns to appropriate types, left padding ZIP codes and FIPS codes with 0's when appropriate, and splitting the geocoded location into two distinct latitude and longitude columns. 

The first step to load the HHS data is to load any new ZIP codes found into the `locations` table. We drop any duplicate ZIP codes found in the new data set, and compare this list to ZIP codes currently in the table to ensure no duplicate rows are added. Then, any rows which meet this criteria and do not have any missing location data (ZIP code, city, or state) are added to the `locations` table. We track and report the number of new rows added to the table in this way. 

The next step is to update the `hospital` table. For each row, we check if the unique identifier `hospital_pk` already exists within the database table. If so, we check the metadata values and update any new or inconsistent information. If the unique identifier does not currently exist within our table, we create a new row in the `hospital` table to represent this new hospital. We track and report the number of new rows added to the table, as well as the number of existing rows that were updated with new information. 

Lastly, we append to the `weekly_logs` table. For this, we simply check the data within each row to see if it meets the constraints imposed by our schema. If so, we insert the row, and otherwise we skip, keeping track of how many rows are inserted and skipped.   

### Loading Quality Data

To load the Quality data into the database, run the script with the following terminal command:

```
python load-quality.py [date_str] [filepath]
```

The first command line argument is the date the Quality data was updated, following the `YYYY-MM-DD` format. Similarly to the HHS data, the script assumes a consistent naming convention for the data file names. For the quality data, file names must be of the form `Hospital_General_Information-YYYY-MM.csv` (for example, `Hospital_General_Information-2021-07.csv`).

This script also follows a similar logic as `load-hhs.py`. It loads the data from a .CSV file and then preprocesses it. Here, preprocessing includes adding a date column to track when the quality ratings were issued, as well as left padding ZIP codes and FIPS codes with 0's when appropriate. 

Before adding any new rows to the `hospital_quality` table, we must go through a similar process to loading the HHS data to update the `location` and `hospital` tables. After doing so, we iterate through each row in the Quality data set, convert the overall hospital quality rating to the enumerated data type defined in the schema, and then insert the row. However, if the hospital information is missing, we skip the row, again keeping track of how many rows were inserted and how many rows were skipped.

## Part 3 - Reporting

The last stage of the pipeline is generating reports.

**[add more about report generation here later]**
