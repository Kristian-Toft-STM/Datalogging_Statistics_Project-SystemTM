from json_functions import *
#from SQLiteWrite import *
from Snap7_Functions import *

import logging
import csv
import time
import sqlite3
import shutil
import os
import glob

from datetime import datetime

# export table data from sql to csv file
def export_sql_to_csv(db_manager):
    try:    

        files = 0
        file_limit = 3

        directory_path = "C:\\Users\\Admin-STM\\logs\\csv"

        # Ensure directory exists
        os.makedirs(directory_path, exist_ok=True)

        csv_files = glob.glob(os.path.join(directory_path, "*.csv"))
        files = len(csv_files)

        print(f"Number of CSV files in the folder: {files}")

        if (files < file_limit):
            table_data = db_manager.get_all_data_from_table()
            temp_timestamp = datetime.now().strftime("%m%d%Y%H%M%S")

            print(f'{str(temp_timestamp)}')

            filename = f'raw_data{temp_timestamp.replace(" ", "")}.csv'
            file_path = os.path.join(directory_path, filename)

            # Writing to the CSV file
            with open(file_path, 'w', newline='') as csvfile:
                writer = csv.writer(csvfile)
                for row in table_data:
                    writer.writerow(row)

        return

    except Exception as e:
        print(e)
        logging.error(f"Export sql to csv error: {e}", exc_info=True)  


# export table data from sql to csv file
def export_sql_to_csv_development(db_manager):
    try:    

        files = 0
        file_limit = 3

        directory_path = os.path.dirname(os.path.abspath(__file__))
        csv_files = glob.glob(os.path.join(directory_path, "*.csv"))
        files = len(csv_files)

        print(f"Number of CSV files in the folder: {files}")

        if (files < file_limit):
            table_data = db_manager.get_all_data_from_table()
            temp_timestamp = datetime.now().strftime("%m%d%Y%H%M%S")

            print(f'{str(temp_timestamp)}')

            filename = f'raw_data{temp_timestamp.replace(" ", "")}.csv'

            # Writing to the CSV file
            with open(filename, 'w', newline='') as csvfile:
                writer = csv.writer(csvfile)
                for row in table_data:
                    writer.writerow(row)
      
        return

    except Exception as e:
        print(e)
        logging.error(f"Export sql to csv error: {e}", exc_info=True)  


# timer for executing export_sql_to_csv()   
def csv_export_timer(db_manager):
    try: 

        export_interval = 30 # seconds

        while True: # loop for continous counting

            start = time.time()

            while time.time() < start + export_interval: 

                print("Time elapsed:", time.time() - start)  
                time.sleep(1) 

            end = time.time()
            length = end - start 
            export_sql_to_csv(db_manager)
            print("It took", length, "seconds!")

    except Exception as e:
        print(e)
        logging.error(f"CSV export timer error: {e}", exc_info=True)    


# delete earlier rown in sql database if database file size is above a certain threshold 
def manage_db_size(db_manager):
    try:   

        size_buffer = 40000000000 # bytes

        total, used, free = shutil.disk_usage("/")
        print("Total: %d GiB" % (total // (2**30)))
        print("Used: %d GiB" % (used // (2**30)))
        print("Free: %d GiB" % (free // (2**30)))
        if db_manager.get_db_size() >= total-size_buffer: # delete data if sql db file size is above total disk space - size_buffer
            conn = sqlite3.connect(f'{db_manager.sql_db_path}')
            cursor = conn.cursor()

            cursor.execute(f"DELETE FROM {db_manager.table_name} WHERE TimeStamp IN (SELECT TimeStamp FROM {db_manager.table_name} ORDER BY TimeStamp ASC LIMIT 10)")

            conn.commit()
            cursor.close()
            conn.close()    
        return  

    except Exception as e:
        print(e)
        logging.error(f"Manage database size error: {e}", exc_info=True)          


def get_folder_size(folder_path):
    try: 
        total_size = 0
        file_size_limit = 100000
        oldest_file = None
        oldest_time = float('inf')
        
        # Walk through all the directories and files
        for dirpath, dirnames, filenames in os.walk(folder_path):
            for filename in filenames:

                file_path = os.path.join(dirpath, filename)
                total_size += os.path.getsize(file_path)

                file_creation_time = os.path.getctime(file_path)

                if file_creation_time < oldest_time:
                    oldest_time = file_creation_time
                    oldest_file = file_path

        if total_size > file_size_limit and oldest_file:
            os.remove(oldest_file)

        return total_size
    
    except Exception as e:
        print(e)
        logging.error(f"Get folder size error: {e}", exc_info=True)        