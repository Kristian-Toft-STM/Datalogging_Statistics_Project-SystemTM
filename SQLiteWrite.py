import sqlite3
import datetime
import tzlocal
from SQLiteRead import get_last_timestamp_from_table, table_exists, table_not_empty
from json_functions import setup_get_sql_column_names_from_file, setup_file_column_names_dict_to_array, get_plc_from_file


#Deprecated when setup_sql_table_from_json() is complete 
""" 
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
        print("Define new table error:", e)
"""

def insert_data_into_table(db_path, table_name, data, setup_file): # udvid til automatisk også at hente table name?
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        column_array = setup_file_column_names_dict_to_array(setup_get_sql_column_names_from_file(setup_file)) 
        column_array = column_array[1:] # remove the first column (timestamp) from the array
        
        #column_string = ",".join(map(str, column_array)) 
        column_string = ",".join(map(lambda column: f"[{column}]", column_array))
        value_placeholders = ",".join(["?" for _ in data])  # create a string of placeholders to protect from sql injections
        
        insert_query = f"INSERT INTO {table_name} ({column_string}) VALUES ({value_placeholders})"

        cursor.execute(insert_query, data)
        conn.commit()
        if (table_exists(db_path, table_name)):
            if (table_not_empty(db_path, table_name)):
                print(f"Data succesfully logged to SQL ({get_last_timestamp_from_table(db_path, table_name)})")
            else:
                print("No data in table")
        else:
                print("Table does not exist")
        cursor.close()
        conn.close()

    except Exception as e:
        print("Insert data into table error:", e)          


def setup_sql_table_from_json(db_path, table_name, setup_file):
    try:

        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        if not table_exists(db_path, table_name):
            # If the table does not exist, create it
            print(f"Table {table_name} does not exist. Creating it...")
            cursor.execute(f"CREATE TABLE {table_name} (Timestamp DATETIME DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY (Timestamp))")
            print(f"Table {table_name} created.")

        if not table_not_empty(db_path, table_name):
            json_setup_column_names = setup_get_sql_column_names_from_file(setup_file)
            for I, dict in enumerate(json_setup_column_names):
                if I == 0:
                    continue
                for key in dict:
                    value = dict[key]
                    cursor.execute(f"ALTER TABLE {table_name} ADD [{value}] VARCHAR(50);")

        cursor.close()
        conn.close()

    except Exception as e:
        print("Setup sql table from json error:", e)              

def delete_table_data(db_path, table):
    try:
        
        conn = sqlite3.connect(f'{db_path}')
        cursor = conn.cursor()

        cursor.execute(f"DELETE FROM {table}")

        conn.commit()
        cursor.close()
        conn.close()

        return
    
    except Exception as e:
        print("Delete table data error:", e)     

def drop_table(db_path, table):
    try:

        conn = sqlite3.connect(f'{db_path}')
        cursor = conn.cursor()

        cursor.execute(f"DROP TABLE {table}")

        conn.commit()
        cursor.close()
        conn.close()

        return
    
    except Exception as e:
        print("Drop table error:", e)   

