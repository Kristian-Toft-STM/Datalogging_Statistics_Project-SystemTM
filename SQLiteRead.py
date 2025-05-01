import sqlite3
import pytz
from datetime import datetime, timedelta
from tzlocal import get_localzone
from json_functions import *
import SQLiteWrite
import logging
from logger import logger
import os

class SQLDatabaseManager:
    def __init__(self, table_name, setup_file, sql_db_path):
        self.table_name = table_name
        self.setup_file = setup_file
        self.sql_db_path = sql_db_path
    

    def sqlite3_connection(self, isolation_level_none=False):
        try:
            conn = sqlite3.connect(self.sql_db_path)
            if (isolation_level_none):
                conn.isolation_level = None # sets connection to auto-commit mode (changes to sqldb can be accessed instantly)
            conn.execute("PRAGMA busy_timeout = 5000")
            conn.execute("PRAGMA read_uncommitted = true")
            cursor = conn.cursor()

            return conn, cursor

        except Exception as e:
            print(e)
            logging.error(f"sqlite connection error: {e}", exc_info=True)

    # check wether a specific table exists in the sql database    
    def table_exists(self):
        try:

            conn, cursor = self.sqlite3_connection()

            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?",(self.table_name,))
            result = cursor.fetchone()

            cursor.close()
            conn.close()
        
            return result is not None

        except Exception as e:
            print(e)
            logger.error(f"Table exists error: {e}", exc_info=True)

    # check wether any table exists in the sql database
    def any_table_exists(self):
        try:

            conn, cursor = self.sqlite3_connection()

            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            result = cursor.fetchone()

            cursor.close()
            conn.close()
        
            return result is not None
        
        except Exception as e:
            print(e)
            logger.error(f"Any table exists error: {e}", exc_info=True)

    # check if a table is not empty / contains a row
    def table_not_empty(self):
        try:

            conn, cursor = self.sqlite3_connection()
            cursor.execute(f"SELECT EXISTS(SELECT 1 FROM {self.table_name});") 
            result, = cursor.fetchone()   
            
            return bool(result)
        
        except Exception as e:
            print(e)
            logger.error(f"Table not empty error: {e}", exc_info=True)

    # get all rows from a specific table
    def get_all_data_from_table(self):
        try:

            conn, cursor = self.sqlite3_connection()

            cursor.execute(f"SELECT * FROM {self.table_name}")

            rows = cursor.fetchall()
            rowsUnTupled = untuple_all_items(rows)

            cursor.close()
            conn.close()
    
            return rowsUnTupled

        except Exception as e:
            print(e)
            logger.error(f"Get all data from table error: {e}", exc_info=True)

    def get_last_24_hours_data_from_table(self, date):
        try:
            end_dt = date.replace(hour=0, minute=0, second=0, microsecond=0) # set time data of datetime to 0 for accurate -24 hour log
            start_dt = (end_dt - timedelta(hours=23, minutes=59, seconds=55)) # set start_dt to 24 hours before end_dt
            start_dt = start_dt.replace(hour=0, minute=0, second=0, microsecond=0)

            logger.info(f'CSV start dt: {start_dt}')
            logger.info(f'CSV end dt: {end_dt}')

            conn, cursor = self.sqlite3_connection()

            cursor.execute(f"SELECT * FROM {self.table_name} WHERE TimeStamp BETWEEN '{start_dt}' AND '{end_dt}';")

            rows = cursor.fetchall()
            logger.info(f"ROWS (RESULT OF cursor.fetchall()): {rows}")

            if rows:
                rows_untupled = untuple_all_items(rows)
                logger.info(f"ROWSUNTUPLED (RESULT OF untuple_all_items(rows)): {rows_untupled}")
            else:
                return 0    

            cursor.close()
            conn.close()

            return rows_untupled
        
        except Exception as e:
            print(e)
            logger.error(f"Get last 24 hours data from table error: {e}", exc_info=True)

    # get the newest timestamp from the sql database table
    def get_last_timestamp_from_table(self):
        try:

            conn, cursor = self.sqlite3_connection()

            cursor.execute(f"SELECT * FROM {self.table_name} ORDER BY TimeStamp DESC LIMIT 1;")
            last_timestamp_row = cursor.fetchone()
            last_timestamp = last_timestamp_row[0]

            cursor.close()
            conn.close()

            return last_timestamp

        except Exception as e:
            print(e)
            logger.error(f"Get last timestamp from table error: {e}", exc_info=True)

    # get the oldest timestamp from the sql database table
    def get_first_timestamp_from_table(self):
        try:

            conn, cursor = self.sqlite3_connection()

            cursor.execute(f"SELECT * FROM {self.table_name} ORDER BY TimeStamp ASC LIMIT 1;")
            last_timestamp_row = cursor.fetchone()
            last_timestamp = last_timestamp_row[0]

            cursor.close()
            conn.close()

            return last_timestamp

        except Exception as e:
            print(e)
            logger.error(f"Get first timestamp from table error: {e}", exc_info=True)        

    # get all timestamp within a range of timestamp
    def get_log_timestamps_within_range(self, min_range, max_range):
        try:

            local_tz = get_localzone()

            min_range_utc = min_range.astimezone(pytz.utc)
            max_range_utc = max_range.astimezone(pytz.utc)

            conn, cursor = self.sqlite3_connection()

            cursor.execute(f"SELECT * FROM {self.table_name} WHERE TimeStamp BETWEEN '{min_range_utc}' AND '{max_range_utc}';")
            data_tuple = cursor.fetchall()

            date_format = "%Y-%m-%d %H:%M:%S"

            data_array = untuple_first_item(data_tuple)

            # converts each timestamp from string to datetime, then utc to local time, then converts back to string according to date_format
            data_array = [pytz.utc.localize(datetime.strptime(data, date_format)).astimezone(local_tz).strftime(date_format) for data in data_array]

            cursor.close()
            conn.close()

            return data_array
        
        except Exception as e:
            print(e)
            logger.error(f"Get logs within range error: {e}", exc_info=True)     

    # get all data within a range of timestamps
    def get_log_data_within_range(self, min_range, max_range=datetime.now(), column=None):
        try:

            min_range_utc = min_range.astimezone(pytz.utc)
            max_range_utc = max_range.astimezone(pytz.utc)

            print(f'From: {min_range_utc}')
            print(f'To: {max_range_utc}')

            conn, cursor = self.sqlite3_connection()

            if column is not None:
                cursor.execute(f"SELECT {column} FROM {self.table_name} WHERE TimeStamp BETWEEN '{min_range_utc}' AND '{max_range_utc}';")     
            else:
                cursor.execute(f"SELECT * FROM {self.table_name} WHERE TimeStamp BETWEEN '{min_range_utc}' AND '{max_range_utc}';")

            data_tuple = cursor.fetchall()
            cursor.close()
            conn.close()

            if column is not None:
                data_array = untuple_first_item(data_tuple) 
                data_array_summed = add_together_single_array_data(data_array)  

            else:
                data_array = untuple_all_excluding_first_item(data_tuple)
                data_array_summed = add_together_array_data(data_array)
                for i, data_point in enumerate(data_array_summed):
                    if data_point > 2147483647:
                        data_array_summed[i] = 2
                        logger.error(f"Get log data within range error: summed data exceeded dint value limit. Data set to 2 \n {data_point}", exc_info=True)

            return data_array_summed
        
        except Exception as e:
            print(e)
            logger.error(f"Get log data within range error: {e}", exc_info=True)     

    # get all data within a range of timestamps, and sum the data in the sql
    def get_log_data_within_range_sql_sum(self, min_range, max_range, column=None):
        try:

            min_range_utc = min_range.astimezone(pytz.utc)
            max_range_utc = max_range.astimezone(pytz.utc)

            conn, cursor = self.sqlite3_connection()

            if column is not None:
                cursor.execute(f"SELECT SUM({column}) FROM {self.table_name} WHERE TimeStamp BETWEEN '{min_range_utc}' AND '{max_range_utc}';")    
            else:
                column_array = setup_file_column_names_dict_to_array(setup_get_sql_column_names_from_file(self.setup_file)) 
                sum_parts = ', '.join([f'SUM("{column}") AS "sum_{column}"' for column in column_array])
                cursor.execute(f"SELECT {sum_parts} FROM {self.table_name} WHERE TimeStamp BETWEEN '{min_range_utc}' AND '{max_range_utc}';")

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
            logger.error(f"Get logs within range sql sum error: {e}", exc_info=True)      

    # get number if rown within a range of timestamps
    def get_number_of_rows_in_range(self, min_range, max_range):
        try:

            timestamp_array = self.get_log_timestamps_within_range(min_range, max_range)
            number_of_rows = len(timestamp_array)
            return number_of_rows

        except Exception as e:
            print(e)
            logger.error(f"Get number of rows in range error: {e}", exc_info=True) 

    # get number of seconds within range of timestamps
    def get_seconds_in_range(self, min_range, max_range):
        try:
            
            timestamp_array = self.get_log_timestamps_within_range(min_range, max_range)
            if len(timestamp_array) > 0:
                first_timestamp = datetime.strptime(timestamp_array[0]  , "%Y-%m-%d %H:%M:%S")
                last_timestamp = datetime.strptime(timestamp_array[-1]  , "%Y-%m-%d %H:%M:%S")  
                print(first_timestamp)
                print(last_timestamp)
                time_delta = last_timestamp - first_timestamp
                time_delta_seconds = time_delta.total_seconds()

                return int(time_delta_seconds)
            return 0
        except Exception as e:
            print(e)
            logger.error(f"Get seconds in range error: {e}", exc_info=True) 

    # check if column exists in table
    def column_exists_in_table(self, target_column):
        try:

            conn, cursor = self.sqlite3_connection()

            cursor.execute(f'SELECT * FROM {self.table_name} LIMIT 0')
            columns = [description[0] for description in cursor.description]

            for column in columns:
                if column == target_column:
                    cursor.close()
                    return True
                
            cursor.close()
            return False
        
        except Exception as e:
            print(e)
            logger.error(f"Column exists in table error: {e}", exc_info=True)    

    # check if column exists in table
    def get_column_names(self):
        try:

            column_names_array = []

            conn, cursor = self.sqlite3_connection()

            cursor.execute(f'SELECT * FROM {self.table_name} LIMIT 0')
            columns = [description[0] for description in cursor.description]

            for column in columns:
                column_names_array.append(column)
                
            cursor.close()

            return column_names_array
        
        except Exception as e:
            print(e)
            logger.error(f"Get column names error: {e}", exc_info=True)            
    
    # get the file size of the sql database 
    def get_db_size(self):
        return os.path.getsize(self.sql_db_path)


    # SQLiteWrite functions
    def delete_table_data(self):
        SQLiteWrite.delete_table_data(self)

    def drop_table(self):
        SQLiteWrite.drop_table(self)

    def sql_add_column(self, target_column):
        SQLiteWrite.sql_add_column(self, target_column)

    def sql_rename_column(self, column, column_name):
        SQLiteWrite.sql_rename_column(self, column, column_name)   

    def sql_drop_column(self, column):
        SQLiteWrite.sql_drop_column(self, column)

    def manage_db_size(self):
        SQLiteWrite.manage_db_size(self)

    def setup_sql_table_from_json(self):
        SQLiteWrite.setup_sql_table_from_json(self)    

    def insert_data_into_table(self, data):    
        SQLiteWrite.insert_data_into_table(self, data)

# get first item of tuple 
def untuple_first_item(tuples):
    try:

        return [item[0] for item in tuples]
    
    except Exception as e:
        print(e)
        logger.error(f"Untuple first item error: {e}", exc_info=True)

# get all items in tuple as an array
def untuple_all_items(tuples):
    try:

        return [list(item) for item in tuples]
    
    except Exception as e:
        print(e)
        logger.error(f"Untuple all error: {e}", exc_info=True)

# get all items in tuple as an array, except first item of tuple
def untuple_all_excluding_first_item(tuples):
    try:

        return [list(item[1:]) for item in tuples]
    
    except Exception as e:
        print(e)
        logger.error(f"Untuple data error: {e}", exc_info=True)

# sum an array into a single variable
def add_together_single_array_data(array_of_data):
    try:

        data_array_summed = sum(array_of_data) 

        return data_array_summed  
     
    except Exception as e:
        print(e)
        logger.error(f"Add together single array data error: {e}", exc_info=True)    

# sum each item of multiple rows into an array
def add_together_array_data(array_of_arrays_of_data):
    try:

        # Use zip to group each ith element of the subarrays together and sum them
        data_array_summed = [sum(group) for group in zip(*array_of_arrays_of_data)] 

        # Exclude the non-sum columns
        # print(F'{array_of_arrays_of_data[:1][0][:1]} \n {array_of_arrays_of_data[:1][0][:2]} \n {array_of_arrays_of_data[:1][0][:3]}')
        # data_array_summed[:1] = array_of_arrays_of_data[:1][0][:1] 
        # data_array_summed[:2] = array_of_arrays_of_data[:1][0][:2]     
        # data_array_summed[:3] = array_of_arrays_of_data[:1][0][:3]    

        return data_array_summed  
     
    except Exception as e:
        print(e)
        logger.error(f"Add together array data error: {e}", exc_info=True)  