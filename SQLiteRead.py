import sqlite3
import pytz
from datetime import datetime
from tzlocal import get_localzone
from json_functions import get_plc_from_file, setup_file_column_names_dict_to_array, setup_get_sql_column_names_from_file
import logging
import os

class SQLDatabaseManager:
    def __init__(self, db_path, table_name, setup_file):
        self.db_path = db_path
        self.table_name = table_name
        self.setup_file = setup_file
    

def table_exists(db_path, table_name):
    try:

        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?",(table_name,))
        result = cursor.fetchone()

        cursor.close()
        conn.close()
    
        return result is not None

    except Exception as e:
        print(e)
        logging.error(f"Table exists error: {e}", exc_info=True)


def any_table_exists(db_path):
    try:

        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        result = cursor.fetchone()

        cursor.close()
        conn.close()
    
        return result is not None
    
    except Exception as e:
        print(e)
        logging.error(f"Any table exists error: {e}", exc_info=True)


def table_not_empty(db_path, table):
     try:

        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute(f"SELECT EXISTS(SELECT 1 FROM {table});") 
        result, = cursor.fetchone()   
           
        return bool(result)
     
     except Exception as e:
        print(e)
        logging.error(f"Table not empty error: {e}", exc_info=True)


def get_all_data_from_table(db_path, table):
    try:

        conn = sqlite3.connect(f'{db_path}')
        cursor = conn.cursor()

        cursor.execute(f"SELECT * FROM {table}")

        rows = cursor.fetchall()
        rowsUnTupled = untuple_all_items(rows)

        cursor.close()
        conn.close()
   
        return rowsUnTupled

    except Exception as e:
        print(e)
        logging.error(f"Get data from table error: {e}", exc_info=True)


def get_last_timestamp_from_table(db_path, table):
     try:

        conn = sqlite3.connect(f'{db_path}')
        cursor = conn.cursor()

        cursor.execute(f"SELECT * FROM {table} ORDER BY TimeStamp DESC LIMIT 1;")
        last_timestamp_row = cursor.fetchone()
        last_timestamp = last_timestamp_row[0]

        cursor.close()
        conn.close()

        return last_timestamp

     except Exception as e:
        print(e)
        logging.error(f"Get last timestamp from table error: {e}", exc_info=True)


def get_log_timestamps_within_range(db_path, table, min_range, max_range):
    try:

        local_tz = get_localzone()

        min_range_utc = min_range.astimezone(pytz.utc)
        max_range_utc = max_range.astimezone(pytz.utc)

        conn = sqlite3.connect(f'{db_path}')
        cursor = conn.cursor()

        cursor.execute(f"SELECT * FROM {table} WHERE TimeStamp BETWEEN '{min_range_utc}' AND '{max_range_utc}';")
        data_tuple = cursor.fetchall()

        date_format = "%Y-%m-%d %H:%M:%S"

        data_array = untuple_first_item(data_tuple)

        # converts each timestamps from string to datetime, then utc to local time, then converts back to string according to date_format 
        data_array = [pytz.utc.localize(datetime.strptime(data, date_format)).astimezone(local_tz).strftime(date_format) for data in data_array]

        cursor.close()
        conn.close()

        return data_array
     
    except Exception as e:
        print(e)
        logging.error(f"Get logs within range error: {e}", exc_info=True)     


def get_log_data_within_range(db_path, table, min_range, max_range, column=None):
    try:

        min_range_utc = min_range.astimezone(pytz.utc)
        max_range_utc = max_range.astimezone(pytz.utc)

        conn = sqlite3.connect(f'{db_path}')
        cursor = conn.cursor()

        if column is not None:
           cursor.execute(f"SELECT {column} FROM {table} WHERE TimeStamp BETWEEN '{min_range_utc}' AND '{max_range_utc}';")     
        else:
            cursor.execute(f"SELECT * FROM {table} WHERE TimeStamp BETWEEN '{min_range_utc}' AND '{max_range_utc}';")

        data_tuple = cursor.fetchall()
        cursor.close()
        conn.close()

        if column is not None:
            data_array = untuple_first_item(data_tuple) 
            data_array_summed = add_together_single_array_data(data_array)  
        else:
            data_array = untuple_all_excluding_first_item(data_tuple)
            data_array_summed = add_together_array_data(data_array)

        return data_array_summed
     
    except Exception as e:
        print(e)
        logging.error(f"Get log data within range error: {e}", exc_info=True)     


def get_log_data_within_range_sql_sum(db_path, table, min_range, max_range, setup_file, column=None):
    try:

        min_range_utc = min_range.astimezone(pytz.utc)
        max_range_utc = max_range.astimezone(pytz.utc)

        conn = sqlite3.connect(f'{db_path}')
        cursor = conn.cursor()

        if column is not None:
            cursor.execute(f"SELECT SUM({column}) FROM {table} WHERE TimeStamp BETWEEN '{min_range_utc}' AND '{max_range_utc}';")    
        else:
            column_array = setup_file_column_names_dict_to_array(setup_get_sql_column_names_from_file(setup_file)) 
            sum_parts = ', '.join([f'SUM("{column}") AS "sum_{column}"' for column in column_array])
            cursor.execute(f"SELECT {sum_parts} FROM {table} WHERE TimeStamp BETWEEN '{min_range_utc}' AND '{max_range_utc}';")

        data_tuple = cursor.fetchall()
        cursor.close()
        conn.close()

        if column is not None:
            data_array = untuple_first_item(data_tuple) 
            data_array_summed = add_together_single_array_data(data_array)  
        else:
            data_array = untuple_all_excluding_first_item(data_tuple)
            data_array_summed = add_together_array_data(data_array)

        return data_array_summed
     
    except Exception as e:
        print(e)
        logging.error(f"Get logs within range sql sum error: {e}", exc_info=True)      


def untuple_first_item(tuples):
    try:

        return [item[0] for item in tuples]
    
    except Exception as e:
        print(e)
        logging.error(f"Untuple first item error: {e}", exc_info=True)


def untuple_all_items(tuples):
    try:

        return [list(item) for item in tuples]
    
    except Exception as e:
        print(e)
        logging.error(f"Untuple all error: {e}", exc_info=True)


def untuple_all_excluding_first_item(tuples):
    try:

        return [list(item[1:]) for item in tuples]
    
    except Exception as e:
        print(e)
        logging.error(f"Untuple data error: {e}", exc_info=True)


def add_together_single_array_data(array_of_data):
    try:

        data_array_summed = sum(array_of_data) 

        return data_array_summed  
     
    except Exception as e:
        print(e)
        logging.error(f"Add together single array data error: {e}", exc_info=True)    


def add_together_array_data(array_of_arrays_of_data):
    try:

        # Use zip to group each ith element of the subarrays together and sum them
        data_array_summed = [sum(group) for group in zip(*array_of_arrays_of_data)]    

        return data_array_summed  
     
    except Exception as e:
        print(e)
        logging.error(f"Add together array data error: {e}", exc_info=True)    


def get_number_of_rows_in_range(db_path, table, min_range, max_range):
    try:

        timestamp_array = get_log_timestamps_within_range(db_path, table, min_range, max_range)
        number_of_rows = len(timestamp_array)
        return number_of_rows

    except Exception as e:
        print(e)
        logging.error(f"Get number of rows in range error: {e}", exc_info=True) 

def get_seconds_in_range(db_path, table, min_range, max_range):
    try:

        timestamp_array = get_log_timestamps_within_range(db_path, table, min_range, max_range)
        first_timestamp = datetime.strptime(timestamp_array[0]  , "%Y-%m-%d %H:%M:%S")
        last_timestamp = datetime.strptime(timestamp_array[-1]  , "%Y-%m-%d %H:%M:%S")  
        time_delta = last_timestamp - first_timestamp
        time_delta_seconds = time_delta.total_seconds()

        return int(time_delta_seconds)

    except Exception as e:
        print(e)
        logging.error(f"Get seconds in range error: {e}", exc_info=True) 

def column_exists_in_table(db_path, table, target_column):
    try:

        conn = sqlite3.connect(f'{db_path}')
        cursor = conn.cursor()

        cursor.execute(f'SELECT * FROM {table} LIMIT 0')
        columns = [description[0] for description in cursor.description]

        for column in columns:
            if column == target_column:
                cursor.close()
                return True
            
        cursor.close()
        return False
    
    except Exception as e:
        print(e)
        logging.error(f"Column exists in table error: {e}", exc_info=True)     

def get_db_size(path_to_db):
    return os.path.getsize(path_to_db)