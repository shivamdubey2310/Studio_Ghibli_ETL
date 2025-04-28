import requests
import pandas as pd
import datetime 
import logging
import pprint
import json
import os
import time

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
        logging.exception(f"An Error occured : {e}")

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
        logging.exception(f"An exception occured while saving data to {sample_file_name} or {raw_file_name}!!!")

    else:
        logging.info(f"Data successfully saved in {sample_file_name} and {raw_file_name}")
    
    logging.info(f"Extraction for {endpoint} completed successfully!!!")


# Extracting data
def Main_extraction():
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