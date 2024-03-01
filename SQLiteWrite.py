import sqlite3
import datetime
from SQLiteRead import get_last_timestamp_from_table

def table_exists(db_path, table_name):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?",(table_name,))
    result = cursor.fetchone()

    cursor.close()
    conn.close()
    
    return result is not None

def define_new_table(db_path, table_name, data_column_name, data_array_length):
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        if not table_exists(db_path, table_name):
            # If the table does not exist, create it
            print(f"Table {table_name} does not exist. Creating it...")
            cursor.execute(f"CREATE TABLE {table_name} (Timestamp DATETIME DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY (Timestamp))")
            print(f"Table {table_name} created.")
            i = 1
            while i < data_array_length + 1:
                cursor.execute(f"ALTER TABLE {table_name} ADD {data_column_name}_{i} VARCHAR(50);")
                i += 1

        cursor.close()
        conn.close()
    except Exception as e:
        print("Define Error:", e)
        
def insert_data_into_table(db_path, table, column, data):
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        column_string = ""
        value_string = ""
        i = 1
        for dat in data:
            column_string += f"{column}_{i},"
            value_string += f"{data[i-1]},"
            i += 1   
        column_string = column_string[:-1]
        value_string = value_string[:-1]  
        insert_string = f"INSERT INTO {table} ({column_string}) VALUES ({value_string})"

        cursor.execute(insert_string)
        print(f"Data succesfully logged to SQL ({get_last_timestamp_from_table(db_path, table)})")

        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        print("Insert Error:", e)        

