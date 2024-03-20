import sqlite3
import datetime
import tzlocal
from SQLiteRead import get_last_timestamp_from_table, table_exists, table_not_empty, any_table_exists, column_exists_in_table, get_db_size
from json_functions import setup_get_sql_column_names_from_file, setup_file_column_names_dict_to_array, get_plc_from_file
import logging


def insert_data_into_table(db_path, table_name, data, setup_file): # udvid til automatisk også at hente table name?
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        column_array = setup_file_column_names_dict_to_array(setup_get_sql_column_names_from_file(setup_file)) 
        column_array = column_array[1:] # remove the first column (timestamp) from the array
        
        #column_string = ",".join(map(str, column_array)) 
        column_string = ",".join(map(lambda column: f"[{column}]", column_array))
        value_placeholders = ",".join(["?" for _ in data])  # create a string of placeholders to protect from sql injections
        print(data)
        insert_query = f"INSERT INTO {table_name} ({column_string}) VALUES ({value_placeholders})"

        cursor.execute(insert_query, data)
        conn.commit()
        cursor. execute('VACUUM;')   
        
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
        print(e)
        logging.error(f"Insert data into table error: {e}", exc_info=True)           


def setup_sql_table_from_json(db_path, table_name, setup_file):
    try:

        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        if not table_exists(db_path, table_name):
            if not any_table_exists(db_path):
                # If the table does not exist, create it
                print(f"Table {table_name} does not exist. Creating it...")
                cursor.execute(f"CREATE TABLE {table_name} (TimeStamp DATETIME DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY (TimeStamp))")
                print(f"Table {table_name} created.")
            else:
                print('Table name has changed, renaming table...')
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
                tables = cursor.fetchall()
                old_table_name = tables[0][0]
                cursor.execute(f"ALTER TABLE {old_table_name} RENAME TO {table_name};")
                print(f'Table "{old_table_name}" renamed to "{table_name}"')

        if not table_not_empty(db_path, table_name):
            json_setup_column_names = setup_get_sql_column_names_from_file(setup_file)
            for I, column_dict in enumerate(json_setup_column_names):
                if I == 0:
                    continue
                for column_key in column_dict:
                    column_value = column_dict[column_key]
                    if not column_exists_in_table(db_path, table_name, column_value):                
                        cursor.execute(f"ALTER TABLE {table_name} ADD [{column_value}] INTEGER;")
        else:
            return

        cursor.close()
        conn.close()

    except Exception as e:
        print(e)
        logging.error(f"Setup sql table from json error: {e}", exc_info=True)               


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
        print(e)
        logging.error(f"Delete table data error: {e}", exc_info=True)      


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
        print(e)
        logging.error(f"Drop table error: {e}", exc_info=True)    


def sql_add_column(db_path, table, column_name):
    try:

        conn = sqlite3.connect(f'{db_path}')
        cursor = conn.cursor()
        
        cursor.execute(f"ALTER TABLE {table} ADD [{column_name}] INTEGER;")

        conn.commit()
        cursor.close()
        conn.close()

        return

    except Exception as e:
        print(e)
        logging.error(f"SQL add column error: {e}", exc_info=True)  


def sql_rename_column(db_path, table, column, column_name):
    try:

        conn = sqlite3.connect(f'{db_path}')
        cursor = conn.cursor()
        
        cursor.execute(f"ALTER TABLE {table} RENAME COLUMN {column} TO {column_name} ;")

        conn.commit()
        cursor.close()
        conn.close()

        return

    except Exception as e:
        print(e)
        logging.error(f"SQL rename column error: {e}", exc_info=True)    


def sql_drop_column(db_path, table, column):
    try:

        conn = sqlite3.connect(f'{db_path}')
        cursor = conn.cursor()
        
        cursor.execute(f"ALTER TABLE {table} DROP COLUMN {column};")

        conn.commit()
        cursor.close()
        conn.close()

        return

    except Exception as e:
        print(e)
        logging.error(f"SQL drop column error: {e}", exc_info=True)    

def manage_db_size(db_path, table_name):
    try: 
                                  #100000000                      
        if get_db_size(db_path) >= 10:
            conn = sqlite3.connect(f'{db_path}')
            cursor = conn.cursor()

            cursor.execute(f"DELETE FROM {table_name} WHERE TimeStamp IN (SELECT TimeStamp FROM {table_name} ORDER BY TimeStamp ASC LIMIT 10)")

            conn.commit()
            cursor.close()
            conn.close()    
        return  

    except Exception as e:
        print(e)
        logging.error(f"manage database size error: {e}", exc_info=True)  