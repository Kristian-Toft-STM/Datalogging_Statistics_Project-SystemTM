from json_functions import get_plc_from_file, setup_file_add_column, setup_file_rename_column, setup_file_delete_column
from SQLiteWrite import sql_add_column, sql_rename_column, sql_drop_column

import logging
import csv
import time
import asyncio

# add a datapoint in the given setup file and in the sql database
def add_datapoint(setup_file_name, datapoint_key, datapoint_key_value, sql_db_path, position):
    try:
        
        table = get_plc_from_file(setup_file_name).get('table name')

        setup_file_add_column(setup_file_name, datapoint_key, datapoint_key_value, position)
        sql_add_column(sql_db_path, table, datapoint_key_value)
        return

    except Exception as e:
        print(e)
        logging.error(f"Add datapoint error: {e}", exc_info=True)

# rename a datapoint in the given setup file and in the sql database
def rename_datapoint(setup_file_name, datapoint_key, datapoint_key_value, sql_db_path):
    try:
        
        table = get_plc_from_file(setup_file_name).get('table name')
        columns = get_plc_from_file(setup_file_name).get('column names')

        for column_dict in columns:
            if datapoint_key in column_dict:
                column = column_dict[datapoint_key]

        setup_file_rename_column(setup_file_name, datapoint_key, datapoint_key_value)
        sql_rename_column(sql_db_path, table, column, datapoint_key_value)
        return

    except Exception as e:
        print(e)
        logging.error(f"Rename datapoint error: {e}", exc_info=True)

# delete a datapoint in the given setup file and in the sql database
def delete_datapoint(setup_file_name, datapoint_key, sql_db_path): 
    try:
        table = get_plc_from_file(setup_file_name).get('table name')
        columns = get_plc_from_file(setup_file_name).get('column names')

        for column_dict in columns:
            if datapoint_key in column_dict:
                column = column_dict[datapoint_key]

        setup_file_delete_column(setup_file_name, datapoint_key)
        sql_drop_column(sql_db_path, table, column)    
        return 
      
    except Exception as e:
        print(e)
        logging.error(f"Delete datapoint error: {e}", exc_info=True)

# export table data from sql to csv file
def export_sql_to_csv(db_manager):
    try:    

        table_data = db_manager.get_all_data_from_table()
        filename = 'raw_data.csv'

        # Writing to the CSV file
        with open(filename, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            for row in table_data:
                writer.writerow(row)
        return
    
    except Exception as e:
        print(e)
        logging.error(f"Export sql to csv error: {e}", exc_info=True)

# timer for executing export_sql_to_csv()   
def csv_export_timer(db_manager):
    try: 

        while True:
            start = time.time()
            # change '5' to actual time (probably 24 hours)
            while time.time() < start + 5:
                print("Time elapsed:", time.time() - start)  
                time.sleep(1) 

            end = time.time()
            length = end - start 
            # export_sql_to_csv(db_manager)
            print("It took", length, "seconds!")

    except Exception as e:
        print(e)
        logging.error(f"CSV export timer error: {e}", exc_info=True)    