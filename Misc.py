from json_functions import *
#from SQLiteWrite import *
from Snap7_Functions import *
from logger import logger

import logging
import csv
import time
import sqlite3
import shutil
import os
import glob

from datetime import datetime, timedelta

# export table data from sql to csv file
def export_sql_to_csv(db_manager):
    try:    

        files = 0
        file_limit = 100
        header = db_manager.get_column_names()
        directory_path = "C:\\Users\\Admin-STM\\logs\\csv"
        error_log_path = "C:\\Users\\Admin-STM\\logs\\error\\error.log"
        data_log_path = "C:\\Users\\Admin-STM\\logs\\data-logs\\data-log.log"

        manage_file_size(error_log_path)
        manage_file_size(data_log_path)

        # Ensure directory exists
        os.makedirs(directory_path, exist_ok=True)

        csv_files = glob.glob(os.path.join(directory_path, "*.csv"))
        files = len(csv_files)

        print(f"Number of CSV files in the folder: {files}")
        logger.info(f"Number of CSV files in the folder: {files}")

        if (files < file_limit):
            table_data = db_manager.get_last_24_hours_data_from_table()
            temp_timestamp = datetime.now().strftime("%m%d%Y%H%M%S")

            print(f'{str(temp_timestamp)}')
            logger.info(f'{str(temp_timestamp)}')

            filename = f'raw_data{temp_timestamp.replace(" ", "")}.csv'
            file_path = os.path.join(directory_path, filename)

            # Writing to the CSV file
            with open(file_path, 'w', newline='') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(header)
                for row in table_data:
                    writer.writerow(row)

        manage_folder_size(directory_path)
        #manage_file_size()

        return

    except Exception as e:
        print(e)
        logger.error(f"Export sql to csv error: {e}", exc_info=True)  


# export table data from sql to csv file
def export_sql_to_csv_development(db_manager):
    try:    

        files = 0
        file_limit = 100
        header = db_manager.get_column_names()
        directory_path = "C:\\Users\\Admin-STM\\logs\\csv"
        error_log_path = "C:\\Users\\Admin-STM\\logs\\error\\error.log"
        data_log_path = "C:\\Users\\Admin-STM\\logs\\data-logs\\data-log.log"

        manage_file_size(error_log_path)
        manage_file_size(data_log_path)

        # Ensure directory exists
        os.makedirs(directory_path, exist_ok=True)

        csv_files = glob.glob(os.path.join(directory_path, "*.csv"))
        files = len(csv_files)

        print(f"Number of CSV files in the folder: {files}")
        logger.info(f"Number of CSV files in the folder: {files}")

        if (files < file_limit):
            table_data = db_manager.get_all_data_from_table()
            temp_timestamp = datetime.now().strftime("%m%d%Y%H%M%S")

            print(f'{str(temp_timestamp)}')
            logger.info(f'{str(temp_timestamp)}')

            filename = f'raw_data{temp_timestamp.replace(" ", "")}.csv'
            file_path = os.path.join(directory_path, filename)

            # Writing to the CSV file
            with open(file_path, 'w', newline='') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(header)
                for row in table_data:
                    writer.writerow(row)

        manage_folder_size(directory_path)
        #manage_file_size()

        return

    except Exception as e:
        print(e)
        logger.error(f"Export sql to csv error: {e}", exc_info=True)  


# timer for executing export_sql_to_csv()   
def csv_export_timer(db_manager):
    try: 

        while True: # loop for continous counting
            now = datetime.now()
            next_midnight = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)

            # Check if we missed midnight (i.e., it's after midnight and the task hasn't run)
            if now.hour >= 0 and now.minute >= 0 and now.second >= 0:
                print(f"It's after midnight ({now}). Executing task now.")
                logger.info(f"It's after midnight ({now}). Executing task now.")
                export_sql_to_csv(db_manager)

            # Calculate how much time is left until the next midnight
            time_until_next_midnight = (next_midnight - now).total_seconds()

            print(f"Next execution at midnight: {next_midnight} (in {time_until_next_midnight} seconds)")
            logger.info(f"Next execution at midnight: {next_midnight} (in {time_until_next_midnight} seconds)")     

            # Sleep until midnight
            time.sleep(time_until_next_midnight)

            export_sql_to_csv(db_manager)

    except Exception as e:
        print(e)
        logger.error(f"CSV export timer error: {e}", exc_info=True)   

# timer for executing export_sql_to_csv()   
def csv_export_timer_old(db_manager):
    try: 

        export_interval = 5 # seconds

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
        logger.error(f"CSV export timer error: {e}", exc_info=True)    


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
        logger.error(f"Manage database size error: {e}", exc_info=True)          


def manage_folder_size(folder_path):
    try: 
        total_size = 0
        file_size_limit = 100000000
        oldest_file = None
        oldest_time = float('inf')
        
        # Walk through all the directories and files
        for dirpath, dirname, filenames in os.walk(folder_path):
            for filename in filenames:

                file_path = os.path.join(dirpath, filename)
                logger.info(f'File size: {os.path.getsize(file_path)}')
                total_size += os.path.getsize(file_path)

                file_creation_time = os.path.getctime(file_path)

                if file_creation_time < oldest_time:
                    oldest_time = file_creation_time
                    oldest_file = file_path

        if total_size > file_size_limit and oldest_file:
            os.remove(oldest_file)
            logger.info(f'File {oldest_file} removed')
            logger.info(f'Folder size: {total_size}')

        return total_size
    
    except Exception as e:
        print(e)
        logger.error(f"Get folder size error: {e}", exc_info=True)        


def manage_file_size(file_path):
    try: 

        file_size_limit = 10000000
        
        file_size = os.path.getsize(file_path)  # Get size in bytes
        
        if(file_size > file_size_limit):
            with open(file_path, 'w'):
                pass

        return file_size
    
    except Exception as e:
        print(e)
        logger.error(f"Get folder size error: {e}", exc_info=True)          


