from json_functions import get_plc_from_file, setup_file_add_column, setup_file_rename_column, setup_file_delete_column
from SQLiteRead import get_all_data_from_table
from SQLiteWrite import sql_add_column, sql_rename_column, sql_drop_column

import logging
import csv
import time
import asyncio

def add_datapoint(setup_file_name, datapoint_key, datapoint_key_value, sql_db_path, position): # maybe make a new script for this and similar functions 
    try:
        
        table = get_plc_from_file(setup_file_name).get('table name')

        setup_file_add_column(setup_file_name, datapoint_key, datapoint_key_value, position)
        sql_add_column(sql_db_path, table, datapoint_key_value)
        return

    except Exception as e:
        print(e)
        logging.error(f"Add datapoint error: {e}", exc_info=True)


def rename_datapoint(setup_file_name, datapoint_key, datapoint_key_value, sql_db_path): # maybe make a new script for this and similar functions 
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


def delete_datapoint(setup_file_name, datapoint_key, sql_db_path): # maybe make a new script for this and similar functions 
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


def export_sql_to_csv(db_path, table_name):
    try:    

        table_data = get_all_data_from_table(db_path, table_name)
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

   
def csv_export_timer(db_path, table_name):
    while True:
        start = time.time()
        # change '5' to actual time (probably 24 hours)
        while time.time() < start + 5:
            print("Time elapsed:", time.time() - start)  
            time.sleep(1) 

        end = time.time()
        length = end - start 
        # export_sql_to_csv(db_path, table_name)
        print("It took", length, "seconds!")

          
