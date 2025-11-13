# Project Prancer
An ETL and reporting pipeline for HHS and Quality hospital data. The pipeline can be broadly broken down into 3 parts:

## Part 1 - Schema

The database schema can be found in the code cell blocks in `project_part_one.ipynb`. Alongside the code, there are textual descriptions of our design decision rationale. We utilize 4 tables to store all relevant information about hospitals:

* **Hospital** - stores all descriptive information and metadata about hospitals that we believe will not change very frequently, if at all. Each record corresponds to one hospital.
* **Weekly Logs** - stores the weekly capacity and use of hospitals. Each row represents one week at one hospital.
* **Quality** - stores the overall hospital quality score assigned to hospitals at various dates. Each row is one quality rating for one hospital.
* **Location** - stores geographic information used to identify and locate hospitals. Each row is one ZIP code.

To create the database, connect to your desired database server in a python script or Jupyter notebook, and then sequentially run the code cells in `project_part_one.ipynb`

## Part 2 - Populating the Database

There are two main python scripts used to load information from local .CSV files into the database tables. These are `load-hhs.py` and `load-quality.py`. These scripts utilize helper functions defined in `utils.py`

### Loading HHS Data

To load the HHS data into the database, run the script with the following terminal command:

```
python load-hhs.py [filepath]
```

The script takes one command line argument, describing where the data is stored on your local machine. The script assumes the data file itself follows the naming convention `YYYY-MM-DD-hhs-data.csv` (for example, `2022-09-23-hhs-data.csv`). Providing a .CSV file with a different naming convention could lead to errors loading the data.

The script first loads the data from the provided .CSV file, and then preprocesses the data. This includes converting data columns to appropriate types, left padding ZIP codes and FIPS codes with 0's when appropriate, and splitting the geocoded location into two distinct latitude and longitude columns. 

**[add more about insertion logic here later]**

### Loading Quality Data

To load the Quality data into the database, run the script with the following terminal command:

```
python load-quality.py [filepath]
```

Similarly to the HHS data, the script assumes a consistent naming convention for the data file names. For the quality data, file names must be of the form `Hospital_General_Information-YYYY-MM.csv` (for example, `Hospital_General_Information-2021-07.csv`).

This script also follows a similar logic as `load-hhs.py`. It loads the data from a .CSV file and then preprocesses it. Here, preprocessing includes adding a date column to track when the quality ratings were issued, as well as left padding ZIP codes and FIPS codes with 0's when appropriate. 

**[add more about insertion logic here later]**

## Reporting

The last stage of the pipeline is generating reports.

**[add more about report generation here later]**
