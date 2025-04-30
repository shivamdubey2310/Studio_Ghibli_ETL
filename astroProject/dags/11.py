import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import logging
import pymongo


# To import all .env variables
from dotenv import load_dotenv
load_dotenv()

def establishing_connection():
    """Establishing connection to mongodb"""

    logging.info("Trying to establish connection")
    myClient = None

    try:
        connection_string = os.getenv("CONNECTION_STRING")
        myClient = pymongo.MongoClient(connection_string)
        # Verifying connection
        db_list = myClient.list_database_names()
    
    except pymongo.errors.ConnectionFailure as e:
        logging.error(f"An error occured while establishing connection : {e}")    
    
    except Exception as e:
        logging.error(f"Any other error occured while establishing connection : {e}")

    else:
        logging.info(f"Connection Established successfully")    

    return myClient

def extraction():
    col = os.getenv("CONNECTION_STRING")
    print(f"aaaaaa - {col}")
    client = establishing_connection()
    print(f"Client = {client}")

with DAG(
    dag_id = "11",
    description = "ETL from studio ghibli API",
    tags = ["ETL", "Mongodb", "API"],
    start_date = datetime(2025, 1, 1),
    schedule_interval = "@daily",
    catchup = False
) as dag:
    
    task_1 = PythonOperator(
        task_id = "Extract",
        python_callable = extraction
    )

    task_1