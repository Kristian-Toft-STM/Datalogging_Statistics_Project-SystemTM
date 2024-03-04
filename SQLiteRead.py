import sqlite3
import pytz
from datetime import datetime
from tzlocal import get_localzone

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

        print(min_range)
        print(max_range)
        min_range_utc = min_range.astimezone(pytz.utc)
        max_range_utc = max_range.astimezone(pytz.utc)
        
        print(min_range_utc)
        print(max_range_utc)

        conn = sqlite3.connect(f'{db_path}')
        cursor = conn.cursor()

        cursor.execute(f"SELECT * FROM {table} WHERE TimeStamp BETWEEN '{min_range_utc}' AND '{max_range_utc}';")
        data_tuple = cursor.fetchall()

        date_format = "%Y-%m-%d %H:%M:%S"

        data_untupled = untuple(data_tuple)

        I=0
        for data in data_untupled:
            data_datetime = pytz.utc.localize(datetime.strptime(data,date_format))
            data_untupled[I] = data_datetime.astimezone(local_tz).strftime(date_format)
            I+=1 

        cursor.close()
        conn.close()

        return data_untupled
     
    except Exception as e:
        print("Get logs within range error:", e)

def untuple(tuple):
    try:
        
        I = 0
        untupled_array = []
        for t in tuple:
            untupled_array.append(t[0])
            I+=1
        return untupled_array  

    except Exception as e:
            print("Untuple error:", e)
