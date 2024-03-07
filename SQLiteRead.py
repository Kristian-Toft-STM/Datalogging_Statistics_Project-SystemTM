import sqlite3
import pytz
from datetime import datetime
from tzlocal import get_localzone
from json_functions import get_plc_from_file

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

def get_data_from_table(db_path, table):
    try:

        conn = sqlite3.connect(f'{db_path}')
        cursor = conn.cursor()

        cursor.execute(f"SELECT * FROM {table}")

        rows = cursor.fetchall() 
        rowsUnTupled = untuple(rows)

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


def get_logs_within_range(db_path, table, min_range, max_range):
    try:

        local_tz = get_localzone()

        min_range_utc = min_range.astimezone(pytz.utc)
        max_range_utc = max_range.astimezone(pytz.utc)

        conn = sqlite3.connect(f'{db_path}')
        cursor = conn.cursor()

        cursor.execute(f"SELECT * FROM {table} WHERE TimeStamp BETWEEN '{min_range_utc}' AND '{max_range_utc}';")
        data_tuple = cursor.fetchall()

        date_format = "%Y-%m-%d %H:%M:%S"

        data_array = untuple(data_tuple)

        # converts each timestamps from string to datetime, then utc to local time, then converts back to string according to date_format 
        data_array = [pytz.utc.localize(datetime.strptime(data, date_format)).astimezone(local_tz).strftime(date_format) for data in data_array]

        cursor.close()
        conn.close()

        return data_array
     
    except Exception as e:
        print("Get logs within range error:", e)     

def untuple(tuples):
    try:

        return [item[0] for item in tuples]
    
    except Exception as e:
        print("Untuple error:", e)