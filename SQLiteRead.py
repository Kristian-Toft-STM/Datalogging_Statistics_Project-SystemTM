import sqlite3
import pytz
from datetime import datetime
from tzlocal import get_localzone
from json_functions import get_plc_from_file, setup_file_column_names_dict_to_array, setup_get_sql_column_names_from_file

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
        print("Table exists error:", e)

def table_not_empty(db_path, table):
     try:

        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute(f"SELECT EXISTS(SELECT 1 FROM {table});") 
        result, = cursor.fetchone()   
           
        return bool(result)
     
     except Exception as e:
        print("Table not empty error:", e)

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
            print("Get data from table error:", e)

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
        print("Get last timestamp from table error:", e)


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
        print("Get logs within range error:", e)     

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
        print("Get logs within range error:", e)     

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
        print("Get logs within range sql sum error:", e)      

def untuple_first_item(tuples):
    try:

        return [item[0] for item in tuples]
    
    except Exception as e:
        print("Untuple first item error:", e)

def untuple_all_items(tuples):
    try:

        return [list(item) for item in tuples]
    
    except Exception as e:
        print("Untuple all error:", e)

def untuple_all_excluding_first_item(tuples):
    try:

        return [list(item[1:]) for item in tuples]
    
    except Exception as e:
        print("Untuple data error:", e)

def add_together_single_array_data(array_of_data):
    try:

        data_as_int = [int(data) for data in array_of_data]
        data_array_summed = sum(data_as_int) 

        return data_array_summed  
     
    except Exception as e:
        print("Add together single array data error:", e)    

def add_together_array_data(array_of_arrays_of_data):
    try:

        data_as_int = [[int(data) for data in array_of_data] for array_of_data in array_of_arrays_of_data]

        # Use zip to group each ith element of the subarrays together and sum them
        data_array_summed = [sum(group) for group in zip(*data_as_int)]    

        return data_array_summed  
     
    except Exception as e:
        print("Add together array data error:", e)    

