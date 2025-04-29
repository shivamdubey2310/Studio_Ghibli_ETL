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

def jsonToDf(file_name):
    """
    Helper function to load a json file to dataframe
    
    Params:
    -------
        file_name(str) : Path of the file
    """
    
    try:
        with open(file_name, "r") as file_reader:
            data_df = pd.read_json(file_reader)
    
    except FileNotFoundError as e:
        logging.error("File {file_name} does not exist!!")
    except Exception as e:
        logging.error(f"An exception occured while opening {file_name} : {e}")
    
    return data_df


def DfToJson(data_df, file_name):
    """
    Helper function to dump a dataframe to json
    
    Params:
    -------
        data_df(dataframe): a Dataframe to dump
        file_name(str) : Path of the file to dump in
    """

    try:
        with open(file_name, "w") as file_writer:
            data_df = pd.read_json(file_writer)
            data_df.to_json(file_writer)
    
    except FileNotFoundError as e:
        logging.error("File {file_name} does not exist!!")
    except Exception as e:
        logging.error(f"An exception occured while opening {file_name} : {e}")


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
    data_df = jsonToDf(file_name)

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
        DfToJson(data_df, file_name)
    logging.info(f"Handling missing values successful for {endpoint}!!")


def handling_duplicate_rows(endpoint):
    """Handling duplicate entries
    params
    ------
        endpoint(str) : Name of the endpoint
    """

    logging.info(f"Handling duplicate values for : {endpoint}")
    
    file_name = f"raw_json/{endpoint}_raw.json"
    data_df = jsonToDf(file_name)
    
    # Removing duplicate entries
    data_df = data_df.drop_duplicates()

    DfToJson(data_df, file_name)

    logging.info(f"Handling duplicate values successful for {endpoint}!!")

def creating_entity_ids(endpoint):
    """Creating new ids (Overwriting older ids)
    Params
    -------
        endpoint(str): name of the endpoint 
    """

    # Saving column_names for every endpoint data
    file_name = f"raw_json/{endpoint}_raw.json"
    data_df = jsonToDf(file_name)

    # Overwriting ids
    old_id_list = list(data_df["id"])
    new_id = 1
    mapping_dict = {}

    for old_id in old_id_list:
        if old_id not in mapping_dict.keys():
            mapping_dict[old_id] = new_id
            new_id += 1
    
    # Overwriting id column to new ids
    data_df["id"] = data_df["id"].map(mapping_dict)

    # Over-Writing raw json 
    file_name = f"raw_json/{endpoint}_raw.json"
    DfToJson(data_df, file_name)

    logging.info(f"ID Overwriting for {endpoint} is successful!!")


def mapping_endpoints(endpoint):
    """
    Establishing relationship between collections by creating a different entity_id and connecting with another collections
    
    Params
    -------
        endpoint(str): name of the endpoint 
    """
    
    # Saving column_names for every endpoint data
    file_name = f"raw_json/{endpoint}_raw.json"
    data_df = jsonToDf(file_name)



def data_cleaning(endpoint):
    """Function to clean data for endpoints"""
    
    # For missing values
    detecting_missing_values(endpoint)
    
    # For duplicate entries
    handling_duplicate_rows(endpoint)

    logging.debug(f"Data cleaning for {endpoint} completed successfully!!")

def transformation():
    """Main transformation function"""

    logging.info("In transformation phase")
    for endpoint in endpoint_list: 
        # data_cleaning(endpoint)

        # For creating another id and relationship
        creating_entity_ids(endpoint)

        # For establishing relationship
        # mapping_endpoints(endpoint)

transformation()


def testing():
    list_1 = ["Shivam", "Nishita", "Shivam", "Hi"]
    df = pd.DataFrame({
        "name": list_1,
        "class" : [1, 12, 23, 55]
        }
    )
    name_list = list(df["name"])
    dict1 = {}
    id = 1
    
    for name in name_list:
        if name not in dict1.keys():
            dict1[name] = id
            id += 1
    
    df["name"] = df["name"].map(dict1)

    print(df)

# testing()