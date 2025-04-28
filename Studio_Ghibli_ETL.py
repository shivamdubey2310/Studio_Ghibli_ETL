import requests
import pandas as pd
import datetime 
import logging
import pprint
import json
import os
import time

# list of endpoints
endpoint_list = [
    "films",
    "people",
    "locations",
    "species",
    "vehicles"
]

# Customizing logging.basicConfig() to format logging 
logging.basicConfig(
    level = logging.DEBUG,
    filename = "ETL_log.log",
    encoding = "utf-8",
    filemode = "a",
    format="{asctime} - {levelname} - {message}",
    style="{",
    datefmt="%Y-%m-%d %H:%M",
)

def Extracting_data(endpoint):
    """Function to extract data from an endpoint
    Params:
    ------
        endpoint(str) : endpoint name to extract data from
        size(int)(default=1) : size of the data to extract
    """

    try:
        logging.info(f"Trying to connect to {endpoint} and decoding to .json()!!")
        response = requests.get(f"https://ghibliapi.vercel.app/{endpoint}/")
        response.raise_for_status()
        response_json = response.json()
    
    except requests.exceptions.JSONDecodeError as e:
        logging.error(f"Unable to decode json for {endpoint} data.")
    
    except Exception as e:
        logging.error(f"An Error occured : {e}")

    else:
        logging.info(f"Connection to {endpoint} and decoding to .json() is successful!!")


    try:
        logging.info(f"Trying to save {endpoint} data to sample directory!!")
        
        # Trying to create a directory and if it exists do nothing (nothing means - don't raise fileExistsException)
        if not os.path.exists("sample_json"):
            os.mkdir("sample_json")
        
        sample_file_name = f"sample_json/{endpoint}_sample.json"
        with open(sample_file_name, "w") as file:
            json.dump(response_json[0], file)

        # Trying to create a directory and if it exists do nothing (nothing means - don't raise fileExistsException)
        if not os.path.exists("raw_json"):
            os.mkdir("raw_json")
        
        raw_file_name = f"raw_json/{endpoint}_raw.json"
        with open(raw_file_name, "w") as file:
            json.dump(response_json, file)

    except Exception as e:
        logging.error(f"An exception occured while saving data to {sample_file_name} or {raw_file_name}!!!")

    else:
        logging.info(f"Data successfully saved in {sample_file_name} and {raw_file_name}")
    
    logging.info(f"Extraction for {endpoint} completed successfully!!!")


# Extracting data
def extraction():
    """Main function to extracte data
    params
    ------
        None
    """
    logging.info("Inside Main_extraction function")

    endpoint_list = [
        "films",
        "people",
        "locations",
        "species",
        "vehicles"
    ]

    # Extracting samples (to be performed only once)
    for endpoint in endpoint_list:
        Extracting_data(endpoint)
        time.sleep(0.5)

    logging.debug("Extraction for all endpoints was successful!!!")

# -----------------------------------------------------------------------------------
def handling_missing_values(data_df, dict_null):
    """Function to handle missing values
    params
    ------
        data_df(pd.df): dataframe to handle null values on
        dict_null(dict) : dictionary of columns and number of missing values
    """

    for key in dict_null.keys():
        column = data_df[key] 
        if str(column.dtype) == "int64":
            data_df[column] = column.fillna(0)
        else:
            data_df[column] = column.fillna("Unknown")
    
    return data_df
    
def detecting_missing_values(endpoint):
    """Function to detect and handle missing values
    params:
    ------
        endpoint(str): name of the endpoint 
    """

    logging.info(f"Checking missing values for : {endpoint}")
    
    # Opening file
    file_name = f"raw_json/{endpoint}_raw.json"
    try:
        with open(file_name, "r") as file_reader:
            data_df = pd.read_json(file_reader)
    except FileNotFoundError as e:
        logging.error("File {file_name} does not exist!!")
    except Exception as e:
        logging.error(f"An exception occured while opening {file_name} : {e}")
    
    # Checking missing values 
    null_value_count = data_df.isna().sum()
    null_value_count_dict = null_value_count.to_dict()
    dict_null = {}  # Dict having count of null values for each column if they have null values

    for key, value in null_value_count_dict.items():
        if value != 0:
            dict_null[key] = value

    if dict_null == {}:
        logging.info(f"There is no null values in {endpoint}!!!")
    else:
        logging.info(f"There are some null values in {endpoint}!!!")
        data_df = handling_missing_values(data_df, dict_null)
        try:
            with open(file_name, "w") as file_writer:
                data_df.to_json(file_writer)
        except Exception as e:
            logging.error(f"An exception occured while writing {file_name} : {e}")

    logging.info(f"Handling missing values successful for {endpoint}!!")

def handling_duplicate_rows(endpoint):
    pass

def data_cleaning(endpoint):
    """Function to clean data for endpoints"""
    detecting_missing_values(endpoint)

    handling_duplicate_rows(endpoint)

def transformation():
    """Main transformation function"""

    logging.info("In transformation phase")
    for endpoint in endpoint_list: 
        data_cleaning(endpoint)

transformation()