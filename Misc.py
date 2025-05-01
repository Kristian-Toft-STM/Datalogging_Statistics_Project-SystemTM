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

directory_path = "C:\\Users\\Admin-STM\\logs\\csv"
error_log_path = "C:\\Users\\Admin-STM\\logs\\error\\error.log"
data_log_path = "C:\\Users\\Admin-STM\\logs\\data-logs\\data-log.log"

# export table data from sql to csv file
def export_sql_to_csv(db_manager, datetime_to_export):
    try:    

        files = 0
        header = db_manager.get_column_names()

        # directory_path = "C:\\Users\\Admin-STM\\logs\\csv"
        # error_log_path = "C:\\Users\\Admin-STM\\logs\\error\\error.log"
        # data_log_path = "C:\\Users\\Admin-STM\\logs\\data-logs\\data-log.log"

        manage_file_size(error_log_path)
        manage_file_size(data_log_path)

        # Ensure directory exists
        os.makedirs(directory_path, exist_ok=True)

        csv_files = glob.glob(os.path.join(directory_path, "*.csv"))
        files = len(csv_files)

        print(f"Number of CSV files in the folder: {files}")
        logger.info(f"Number of CSV files in the folder: {files}")

        
        table_data = db_manager.get_last_24_hours_data_from_table(datetime_to_export)
        logger.info(f"TABLE DATA (RESULT OF GET LAST 24): {table_data}")
        if table_data == 0:
            return

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
            logger.info(f"now var i csv_export_timer: {now}")
            now_trimmed = now.replace(hour=0, minute=0, second=0, microsecond=0)
            logger.info(f"now_trimmed var i csv_export_timer: {now_trimmed}")
            latest_file = get_latest_file_from_folder(directory_path)

            if not latest_file:
                all_days = generate_date_range(db_manager.get_first_timestamp_from_table(), db_manager.get_last_timestamp_from_table())
                for day in all_days:
                    export_sql_to_csv(db_manager, day)

            latest_file_formatted = format_csv_file_name_to_datetime(latest_file)

            next_midnight = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
            
            # Check if we missed midnight (i.e., it's after midnight and the task hasn't run)
            if now.hour >= 0 and now.minute >= 0 and now.second >= 15:

                print(f"It's after midnight ({now}). Executing task now!")
                logger.info(f"It's after midnight ({now}). Executing task now!")

                
                missing_days_array = generate_date_range(latest_file_formatted, now_trimmed)
                
                logger.info(f"missing days: {missing_days_array}")
                

                for missing_day in missing_days_array:
                    export_sql_to_csv(db_manager, missing_day)

            # Calculate how much time is left until the next midnight
            time_until_next_midnight = (next_midnight - now).total_seconds()

            print(f"Next execution at midnight: {next_midnight} (in {time_until_next_midnight} seconds)")
            logger.info(f"Next execution at midnight: {next_midnight} (in {time_until_next_midnight} seconds)")     

            # Sleep until midnight
            time.sleep(time_until_next_midnight)

            # update now timestamp
            now = datetime.now()
            now_trimmed = now.replace(hour=0, minute=0, second=0, microsecond=0)

            if not (latest_file == now_trimmed):
                export_sql_to_csv(db_manager, now_trimmed)

    except Exception as e:
        print(e)
        logger.error(f"CSV export timer error: {e}", exc_info=True)   

def get_latest_file_from_folder(folder_path):
    try:

        files = [os.path.join(folder_path, f) for f in os.listdir(folder_path) if os.path.isfile(os.path.join(folder_path, f))]
        
        # Check if there are any files in the folder
        if not files:
            print("No files found in the folder.")
            return None
        
        # Find the latest file based on modification time
        latest_file = max(files, key=os.path.getmtime)
        
        # Extract the first letter of the file name (not including the path)
        latest_file_day = os.path.basename(latest_file)
        return latest_file_day

    except Exception as e:
        print(e)
        logger.error(f"a: {e}", exc_info=True)    

def format_csv_file_name_to_datetime(file_name):
    try:
       
        file_name_trimmed = file_name.replace('.csv', '') # Remove .csv sufix
        # Extracting the date and time part from the file name (assuming format is 'raw_datammddyyyyhhmmss')
        date_time_str = file_name_trimmed.split('raw_data')[1]  # Remove 'raw_data' prefix
        # Converting the date and time string to a datetime object
        formatted_datetime = datetime.strptime(date_time_str, "%m%d%Y%H%M%S")
        return formatted_datetime
    
    except Exception as e:
        print(f"Error: {e}")
        return None

def generate_date_range(start_date, end_date):
    try:
        # Ensure start_date is earlier than end_date
        if start_date > end_date:
            start_date, end_date = end_date, start_date

        logger.info(f"generate_date_range start_date = {start_date}")    
        logger.info(f"generate_date_range end_date = {end_date}")   

        date_range = []
        current_date = start_date + timedelta(days=1) # Start from the next day

        while current_date <= end_date:
            date_range.append(current_date)
            current_date += timedelta(days=1)

        return date_range
           
    except Exception as e:
        print(f"Error: {e}")
        return None

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


def manage_folder_size_old(folder_path):
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

def manage_folder_size(folder_path):
    try:
        file_size_limit = 100000000  # 100 MB
        total_size = 0

        while True:
            total_size = 0
            file_info = []

            # Walk through all the directories and files to calculate size and track files
            for dirpath, dirname, filenames in os.walk(folder_path):
                for filename in filenames:
                    file_path = os.path.join(dirpath, filename)
                    logger.info(f'File size: {os.path.getsize(file_path)}')
                    file_size = os.path.getsize(file_path)
                    file_creation_time = os.path.getctime(file_path)

                    total_size += file_size
                    file_info.append((file_creation_time, file_size, file_path))

            if total_size <= file_size_limit:
                break  # Exit loop if folder size is within the limit

            # Find the oldest file
            if file_info:
                file_info.sort()  # Sort by creation time (oldest first)
                oldest_file = file_info[0][2]  # Get the file path of the oldest file
                oldest_file_size = file_info[0][1]

                os.remove(oldest_file)  # Remove the oldest file
                total_size -= oldest_file_size

                logger.info(f"Removed file: {oldest_file} (Size: {oldest_file_size} bytes)")
                logger.info(f"Updated folder size: {total_size} bytes")

        return total_size

    except Exception as e:
        logger.error(f"Error while managing folder size: {e}", exc_info=True)
        raise


def manage_file_size(file_path):
    try: 

        file_size_limit = 1000000000
        
        file_size = os.path.getsize(file_path)  # Get size in bytes
        
        if(file_size > file_size_limit):
            with open(file_path, 'w'):
                pass

        return file_size
    
    except Exception as e:
        print(e)
        logger.error(f"Get folder size error: {e}", exc_info=True)          


