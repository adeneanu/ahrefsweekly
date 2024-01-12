# The endpoint URL for the GoDaddy CaaS API
import requests
import json
import mysql.connector
from mysql.connector import Error
import numpy as np
import random
import json
from collections import Counter
from datetime import datetime



def connect_to_db(host, username, password, db_name):
    try:
        connection = mysql.connector.connect(
            host=host,
            user=username,
            password=password,
            database=db_name
        )
        if connection.is_connected():
            print("Successfully connected to the database")
            return connection
    except Error as e:
        print(f"Error while connecting to MySQL: {e}")
        return None


def load_json_from_file(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            data = json.load(file)
            return data
    except FileNotFoundError:
        print(f"The file {file_path} was not found.")
        return None
    except json.JSONDecodeError:
        print(f"The file {file_path} does not contain valid JSON.")
        return None
    except Exception as e:
        print(f"An error occurred: {e}")
        return None


def get_blk_fols(connection):
    try:
        cursor = connection.cursor()
        cursor.execute("SELECT code FROM blacklistf")
        rows = cursor.fetchall()

        # Getting column names from cursor.description
        columns = [col[0] for col in cursor.description]

        # Convert rows to list of dictionaries
        main_report = []
        for row in rows:
            # Decode bytearray to string if necessary
            decoded_row = [col.decode('utf-8') if isinstance(col, bytearray) else col for col in row]
            # Create a dictionary with column names as keys and row data as values
            row_dict = dict(zip(columns, decoded_row))
            main_report.append(row_dict)


        return main_report

    except Error as e:
        print(f"Failed to retrieve data from 'report' table: {e}")
        return None
    finally:
        if cursor is not None:
            cursor.close()


### CONNECT OT THE DB ###
host = '92.205.189.212'
db_name = 'seotoolz_reports'
username = 'seotoolz_reports'
password = '2022GOSearchSEO77#'
connection = connect_to_db(host, username, password, db_name)

blk_fols=get_blk_fols(connection)
blk_fols_list=[]
brand="GDUS"
for blkfol in blk_fols:
    if brand in blkfol['code']:
        blk_fols_list.append(blkfol['code'].replace(brand,''))

connection.close()