# sub-script imports
from SQLiteWrite import insert_data_into_table, delete_table_data, drop_table, setup_sql_table_from_json, sql_rename_column, sql_drop_column, sql_add_column, manage_db_size
from SQLiteRead import get_all_data_from_table, get_log_timestamps_within_range, get_log_data_within_range, get_log_data_within_range_sql_sum, any_table_exists, get_seconds_in_range, get_db_size
from OPCUA_Functions import connect_opcua_client, disconnect_opcua_client, read_node_value, monitor_and_get_data_on_trigger_opcua, monitor_and_insert_data_opcua
from Snap7_Functions import connect_snap7_client, disconnect_snap7_client, get_data_from_plc_db, get_data_array_from_plc_db, monitor_and_get_data_on_trigger_snap7, monitor_and_insert_data_snap7, write_data_dbresult
from json_functions import setup_get_sql_column_names_from_file, setup_file_column_names_dict_to_array, get_plc_from_file, setup_file_get_number_of_data_columns, read_setup_file, setup_file_keys_changed, setup_file_rename_column, setup_file_delete_column, setup_file_add_column, setup_file_delete_or_rename, save_previous_setup_step7, load_previous_setup_step7, insert_list_of_column_names_from_txt_into_json
from Misc import csv_export_timer, export_sql_to_csv

# library imports
from opcua import ua
from tzlocal import get_localzone
import datetime
import pytz
import os
import logging
import inspect
import asyncio
from multiprocessing import Process

# classes
class TableNotFoundError(Exception):
       def __init__(self, message="Table not found", *args):
        caller_frame = inspect.stack()[1]
        caller_func_name = caller_frame.function
        line_number = caller_frame.lineno
        full_message = f"{caller_func_name} at line {line_number}: {message}"
        super().__init__(full_message, *args)

# setup logging
logging.basicConfig(level=logging.ERROR, filename='error.log', format='%(asctime)s - %(levelname)s - %(message)s')
    # bash command to open log file: 'tail -f /path/to/your/log/file/app.log'

# test variables
plc_trigger_id = "ns=4;i=3"
data_node_id = "ns=4;i=4"
sql_db_path = "projekttestDB.db" 
setup_file_opcua = "setup_opcua.json"
setup_file_step7 = "setup_step7.json"

previous_setup_file = None

local_tz = get_localzone()
test_min_range = datetime.datetime(2024,3,12,13,0,0).astimezone(local_tz)
test_max_range = datetime.datetime(2024,3,30,23,0,0).astimezone(local_tz)
test_tid = any
#test_max_range = datetime.datetime.now()

setup_file_to_run = ''
table_name = ''
         
# main functions
def start_init():
    try:

        global setup_file_to_run
        setup_file_to_run = step7_or_opcua_switch(setup_file_step7)

        init()
        
        return
    
    except Exception as e:
        print(e)
        logging.error(f"start init error: {e}", exc_info=True)

def start_main():
    try:

        while True:
            main_script_process = Process(target=main_script)
            #csv_export_timer_process = Process(target=csv_export_timer, args=(sql_db_path, table_name))

            main_script_process.start()
            #csv_export_timer_process.start()

            #main_script()
            #csv_export_timer(sql_db_path, table_name)
            return
        
    except Exception as e:
        print(e)
        logging.error(f"start main error: {e}", exc_info=True)

def step7_or_opcua_switch(file_to_run):
    try:

        script_directory = os.path.dirname(__file__)
        directory_contents = os.listdir(script_directory)
        if 'setup_opcua.json' in directory_contents and 'setup_step7.json' in directory_contents:
            if file_to_run == 'setup_step7.json':
                return 1
            elif file_to_run == 'setup_opcua.json':
                return 2
            else:
                return 3
        elif 'setup_step7.json' in directory_contents and not 'setup_opcua.json' in directory_contents:
            return 1
        elif 'setup_opcua.json' in directory_contents and not 'setup_step7.json' in directory_contents:
            return 2
        else:
            return 0
        
    except Exception as e:
        print(e)
        logging.error(f"Step7 or opcua switch error: {e}", exc_info=True)


def main_script_opcua_start(plc_trigger_id, data_node_id, sql_db_path, setup_file_opcua):
    try:
        
        table_name = get_plc_from_file(setup_file_opcua).get('table name')
        setup_sql_table_from_json(sql_db_path, table_name, setup_file_opcua)
        monitor_and_insert_data_opcua(sql_db_path, plc_trigger_id, table_name, data_node_id, setup_file_opcua)

    except Exception as e:
        print(e)
        logging.error(f"Main opcua script error: {e}", exc_info=True)


def main_script_snap7_start(sql_db_path, setup_file_step7): # current standard for error handling
    try:

        monitor_count = 1
        while monitor_count <= 10:

            monitor_and_insert_data_snap7(sql_db_path, table_name, setup_file_step7, test_min_range, test_max_range)

            print(f"Monitor count: {monitor_count}")
            monitor_count += 1

    except Exception as e:
        print(e)
        logging.error(f"Main snap7 script error: {e}", exc_info=True)


def main_script():
    try:    

        match setup_file_to_run:
            case 0:
                print('No setup file found')
            case 1:
                main_script_snap7_start(sql_db_path, setup_file_step7)
            case 2:
                main_script_opcua_start(plc_trigger_id, data_node_id, sql_db_path, setup_file_opcua)
            case 3:
                print('Multiple setup files found and no file specified.')    
            case _:
                print('Case mismatch')

    except Exception as e:
        print(e)
        logging.error(f"Main script error: {e}", exc_info=True)


def init():
    try:    

        match setup_file_to_run:
            case 0:
                print('No setup file found')
            case 1:
                initialization_step7()
            case 2:
                initialization_opcua()
            case 3:
                print('Multiple setup files found and no file specified.')    
            case _:
                print('Case mismatch')

    except Exception as e:
        print(e)
        logging.error(f"Initialization error: {e}", exc_info=True)   


def initialization_step7():
    try:
        global previous_setup_file
        global table_name
        setup = read_setup_file(setup_file_step7)
        previous_setup_file = setup
        table_name = get_plc_from_file(setup_file_step7).get('table name')

        if table_name is None:
            raise TableNotFoundError(f"Table name not found in setup file: {setup_file_step7}")

        setup_sql_table_from_json(sql_db_path, table_name, setup_file_step7)
        return

    except TableNotFoundError as e:
        logging.error(e)


def initialization_opcua():
    return


def reinitialize_setup():
    try: 
       
        delete_table_data(sql_db_path, table_name)
        drop_table(table_name)
        init()
        return  

    except Exception as e:
        print(e)
        logging.error(f"Reinitialize setup error: {e}", exc_info=True)     


start_init()
#if __name__ == '__main__':
    #start_main()

#print(insert_list_of_column_names_from_txt_into_json('column_names.txt', setup_file_step7))

#print(get_db_size(sql_db_path))

#print(get_db_size(sql_db_path))

#csv_export_timer(sql_db_path, table_name)

#manage_db_size(sql_db_path)

#main_script(setup_file_step7)

#export_sql_to_csv(sql_db_path, 'Test_Table')

#print(get_seconds_in_range(sql_db_path, 'Test_Table', test_min_range, test_max_range))

#rename_datapoint(setup_file_step7, 'Column 3', 'rename_test', sql_db_path)
#delete_datapoint(setup_file_step7, 'Column 3', sql_db_path)
#add_datapoint(setup_file_step7, 'Column 3', 'add_test', sql_db_path, 2)

#main_script_opcua_start(plc_trigger_id, data_node_id, sql_db_path, test_table, test_column_name, setup_file_opcua)
#main_script_snap7_start(sql_db_path, dbinsert_number, test_table, test_column_name, setup_file_step7)

#delete_table_data(sql_db_path, test_table) 
#drop_table(sql_db_path, test_table)         
#print(get_log_timestamps_within_range(sql_db_path, 'Test_Table', test_min_range, test_max_range))
#print(get_log_data_within_range(sql_db_path, 'Test_Table', test_min_range, test_max_range, 'Test_Data_Column_1'))
#print(get_log_data_within_range(sql_db_path, 'Test_Table', test_min_range, test_max_range))
#print(get_all_data_from_table(sql_db_path, 'Test_Table'))        

#print(read_setup_file())
#print(map_node(0))

#client = connect_snap7_client(setup_file_step7)
#print(get_data_array_from_plc_db(1, client, setup_file_step7)) 
#print(client.db_read(1, 0, 1008))

#print(get_data_array_from_db(dbinsert_number, client))        
#monitor_and_get_data_on_trigger_Snap7(dbinsert_number, client)

#map_sql_columns_step7(setup_file_step7)
#print(setup_get_sql_column_names_from_file(setup_file_step7))
#print(setup_file_column_names_dict_to_array(setup_get_sql_column_names_from_file(setup_file_step7)))
#columns_array = setup_file_column_names_dict_to_array(setup_get_sql_column_names_from_file(setup_file_step7)) 
#print(tuple(columns_array))
             
#print(get_dbinsert_number_from_file(setup_file_step7))    
#write_data_dbresult(setup_file_step7)
#print(get_log_data_within_range_sql_sum(sql_db_path, 'Test_Table', test_min_range, test_max_range, setup_file_step7))

#print(get_log_data_within_range(sql_db_path, 'Test_Table', test_min_range, test_max_range))

#write_data_dbresult(setup_file_step7, sql_db_path, table_name, test_min_range, test_max_range)    

#setup_sql_table_from_json(sql_db_path, 'Test_Table', setup_file_step7)
        
#print(setup_file_get_number_of_data_columns(setup_file_step7))
#print(read_setup_file(setup_file_step7))
#print(any_table_exists(sql_db_path))
        
#setup_file_rename_or_delete(setup_file_step7, 'column 4')        
#setup_file_rename_or_delete(setup_file_step7, 'column 5')   
        
#setup_file_keys_changed(setup_file_step7, previous_setup_file)
