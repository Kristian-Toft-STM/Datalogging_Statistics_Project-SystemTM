import sqlite3
import shutil
from json_functions import *
from Snap7_Functions import *
import logging

# insert data into table
def insert_data_into_table(db_manager, data): # udvid til automatisk også at hente table name?
    try:
        conn = sqlite3.connect(db_manager.sql_db_path)
        conn.isolation_level = None # sets connection to auto-commit mode (changes to sqldb can be accessed instantly)
        cursor = conn.cursor()
        
        column_array = setup_file_column_names_dict_to_array(setup_get_sql_column_names_from_file(db_manager.setup_file)) 
        column_array = column_array[1:] # remove the first column (timestamp) from the array
        plc = get_plc_from_file(db_manager.setup_file)  

        statistics_insert_db_number = plc.get('statistics_insert_db number')
        time_current_index = plc.get('time_current index')
        current_dtl_datetime = get_and_format_dtl_bytearray(statistics_insert_db_number, time_current_index)

        manage_db_size(db_manager)

        if (str(current_dtl_datetime) != db_manager.get_last_timestamp_from_table()) and (current_dtl_datetime != 0): 
            print(f"Current time: {current_dtl_datetime}")  

            column_string = ",".join(map(lambda column: f"[{column}]", column_array))
            value_placeholders = ",".join(["?" for _ in data])  # create a string of placeholders to protect from sql injections

            insert_query = f"INSERT INTO {db_manager.table_name} (TimeStamp, {column_string}) VALUES ('{current_dtl_datetime}',{value_placeholders})"

            cursor.execute(insert_query, data)
            conn.commit()
            cursor. execute('VACUUM;') 

            if (db_manager.table_exists()):
                if (db_manager.table_not_empty()):
                    print(f"Data succesfully logged to SQL ({db_manager.get_last_timestamp_from_table()})")
                else:
                    print("No data in table")
            else:
                print("Table does not exist")        
        else:
            print("Error: Timestamp already exists in database. Skipping log...")      
              
        cursor.close()
        conn.close()
        return data
    except Exception as e:
        print(e)
        logging.error(f"Insert data into table error: {e}", exc_info=True)           

# setup the sql database table according to the json setup file
def setup_sql_table_from_json(db_manager):
    try:

        conn = sqlite3.connect(db_manager.sql_db_path)
        cursor = conn.cursor()
        
        if not db_manager.table_exists():
            if not db_manager.any_table_exists():
                # If the table does not exist, create it
                print(f"Table {db_manager.table_name} does not exist. Creating it...")
                cursor.execute(f"CREATE TABLE {db_manager.table_name} (TimeStamp DATETIME NOT NULL, PRIMARY KEY (TimeStamp))")
                print(f"Table {db_manager.table_name} created.")
            else:
                print('Table name has changed, renaming table...')
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
                tables = cursor.fetchall()
                old_table_name = tables[0][0]
                cursor.execute(f"ALTER TABLE {old_table_name} RENAME TO {db_manager.table_name};")
                print(f'Table "{old_table_name}" renamed to "{db_manager.table_name}"')

        if not db_manager.table_not_empty():
            json_setup_column_names = setup_get_sql_column_names_from_file(db_manager.setup_file)
            for I, column_dict in enumerate(json_setup_column_names):
                if I == 0:
                    continue
                for column_key in column_dict:
                    column_value = column_dict[column_key]
                    if not db_manager.column_exists_in_table(column_value):                
                        cursor.execute(f"ALTER TABLE {db_manager.table_name} ADD [{column_value}] INTEGER NOT NULL;")
        else:
            return

        cursor.close()
        conn.close()

    except Exception as e:
        print(e)
        logging.error(f"Setup sql table from json error: {e}", exc_info=True)               

# delete all data in table
def delete_table_data(db_manager):
    try:
        
        conn = sqlite3.connect(f'{db_manager.sql_db_path}')
        cursor = conn.cursor()

        cursor.execute(f"DELETE FROM {db_manager.table_name}")

        conn.commit()
        cursor.close()
        conn.close()

        return
    
    except Exception as e:
        print(e)
        logging.error(f"Delete table data error: {e}", exc_info=True)      

# delete table
def drop_table(db_manager):
    try:

        conn = sqlite3.connect(f'{db_manager.sql_db_path}')
        cursor = conn.cursor()

        cursor.execute(f"DROP TABLE {db_manager.table_name}")

        conn.commit()
        cursor.close()
        conn.close()

        return
    
    except Exception as e:
        print(e)
        logging.error(f"Drop table error: {e}", exc_info=True)    

# add column to sql database table
def sql_add_column(db_manager, column_name):
    try:

        conn = sqlite3.connect(f'{db_manager.sql_db_path}')
        cursor = conn.cursor()
        
        cursor.execute(f"ALTER TABLE {db_manager.table_name} ADD [{column_name}] INTEGER;")

        conn.commit()
        cursor.close()
        conn.close()

        return

    except Exception as e:
        print(e)
        logging.error(f"SQL add column error: {e}", exc_info=True)  

# rename column in sql database table
def sql_rename_column(db_manager, column, column_name):
    try:

        conn = sqlite3.connect(f'{db_manager.sql_db_path}')
        cursor = conn.cursor()
        
        cursor.execute(f"ALTER TABLE {db_manager.table_name} RENAME COLUMN {column} TO {column_name} ;")

        conn.commit()
        cursor.close()
        conn.close()

        return

    except Exception as e:
        print(e)
        logging.error(f"SQL rename column error: {e}", exc_info=True)    

# delete column in sql database table
def sql_drop_column(db_manager, column):
    try:

        conn = sqlite3.connect(f'{db_manager.sql_db_path}')
        cursor = conn.cursor()
        
        cursor.execute(f"ALTER TABLE {db_manager.table_name} DROP COLUMN {column};")

        conn.commit()
        cursor.close()
        conn.close()

        return

    except Exception as e:
        print(e)
        logging.error(f"SQL drop column error: {e}", exc_info=True)    

# delete earlier rown in sql database if database file size is above a certain threshold 
def manage_db_size(db_manager):
    try:       
        total, used, free = shutil.disk_usage("/")
        print("Total: %d GiB" % (total // (2**30)))
        print("Used: %d GiB" % (used // (2**30)))
        print("Free: %d GiB" % (free // (2**30)))
        if db_manager.get_db_size() >= total-40000000000: # true if sql db file size is above total disk space -40gb
            conn = sqlite3.connect(f'{db_manager.sql_db_path}')
            cursor = conn.cursor()

            cursor.execute(f"DELETE FROM {db_manager.table_name} WHERE TimeStamp IN (SELECT TimeStamp FROM {db_manager.table_name} ORDER BY TimeStamp ASC LIMIT 10)")

            conn.commit()
            cursor.close()
            conn.close()    
        return  

    except Exception as e:
        print(e)
        logging.error(f"Manage database size error: {e}", exc_info=True)  