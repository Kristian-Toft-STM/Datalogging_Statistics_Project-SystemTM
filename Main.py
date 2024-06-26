# ------------------------------------------------------ IMPORTS ------------------------------------------------------
# sub-script imports
from OPCUA_Functions import *
from Snap7_Functions import *
from json_functions import *
from Misc import *
from test import *
from SQLiteRead import SQLDatabaseManager

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

# ------------------------------------------------------ CLASSES ------------------------------------------------------
# for error handling
class TableNotFoundError(Exception):
       def __init__(self, message="Table not found", *args):
        caller_frame = inspect.stack()[1]
        caller_func_name = caller_frame.function
        line_number = caller_frame.lineno
        full_message = f"{caller_func_name} at line {line_number}: {message}"
        super().__init__(full_message, *args)
  
# ------------------------------------------------------ ERROR LOG ------------------------------------------------------
# setup logging
logging.basicConfig(level=logging.ERROR, filename='error.log', format='%(asctime)s - %(levelname)s - %(message)s')
    # bash command to open log file: 'tail -f /path/to/your/log/file/app.log'

# ------------------------------------------------------ TEST VARIABLES ------------------------------------------------------
plc_trigger_id = "ns=4;i=3"
data_node_id = "ns=4;i=4" 
#sql_db_path = "projekttestDB.db" 
setup_file_opcua = "setup_opcua.json"
setup_file_step7 = "setup_step7.json"
local_tz = get_localzone() 
test_min_range = datetime.datetime(2024,3,12,13,0,0).astimezone(local_tz)
test_max_range = datetime.datetime(2024,3,30,23,0,0).astimezone(local_tz)
test_tid = any
#test_max_range = datetime.datetime.now()
#table_name = ''

# ------------------------------------------------------ GLOBAL VARIABLES ------------------------------------------------------
db_manager = SQLDatabaseManager('','','') # initialise SQL database manager object, for handling database operations  
previous_setup_file = None # variable for storing the previous setup file
setup_file_to_run = '' # global variable for storing target setup file

# ------------------------------------------------------ MAIN FUNCTIONS ------------------------------------------------------
# start initialization
def start_init():
    try:

        global setup_file_to_run
        setup_file_to_run = step7_or_opcua_switch(setup_file_step7) # choose which setup file to run
        init() # run initialisation depending on type of setup file
        
        return
    
    except Exception as e:
        print(e)
        logging.error(f"start init error: {e}", exc_info=True)

# start main functionality procs, running asynchronously to eachother
def start_main():
    try:
        main_script_proc = Process(target=main_script) # main functionality, including cyclically logging to sql database
        write_data_dbresult_proc = Process(target=write_data_dbresult, args=(db_manager,)) # monitor requests for writing data to plc
        #csv_export_timer_proc = Process(target=csv_export_timer, args=(sql_db_path, table_name)) # csv export timer

        main_script_proc.start()
        write_data_dbresult_proc.start()
        #csv_export_timer_proc.start()

        #main_script()
        #csv_export_timer(sql_db_path, table_name)

        return
        
    except Exception as e:
        print(e)
        logging.error(f"start main error: {e}", exc_info=True)

# check setup file/s in projekt folder, and set setup file variable accordingly 
def step7_or_opcua_switch(file_to_run):
    try:

        script_directory = os.path.dirname(__file__) # get projekt directory path
        directory_contents = os.listdir(script_directory) # get content of directory

        # search for specific setup file names in directory, and return a corresponding number
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

# main script for opcua communication
def main_script_opcua_start(plc_trigger_id, data_node_id, sql_db_path, setup_file_opcua):
    try:
        
        table_name = db_manager.table_name
        db_manager.setup_sql_table_from_json()
        monitor_and_insert_data_opcua(sql_db_path, plc_trigger_id, table_name, data_node_id, setup_file_opcua)

    except Exception as e:
        print(e)
        logging.error(f"Main opcua script error: {e}", exc_info=True)

# main script for step7/snap7 communication
def main_script_snap7_start():
    try:

        # loop for continously monitor for logging requests, cycles limited by monitor_counter
        monitor_count = 1
        while monitor_count <= 200000000000:
            monitor_and_insert_data_snap7(db_manager, test_max_range) # monitor for logging requests

            print(f"Monitor count: {monitor_count}")
            monitor_count += 1
            time.sleep(0.5)

    except Exception as e:
        print(e)
        logging.error(f"Main snap7 script error: {e}", exc_info=True)

# case for deciding which main script to run, depending on the setup file
def main_script():
    try:    

        match setup_file_to_run:
            case 0:
                print('No setup file found')
            case 1:
                print('Step 7 setup found, running it now...')
                main_script_snap7_start()
            case 2:
                pass
                print('Opcua setup found, running it now...')
                #main_script_opcua_start(plc_trigger_id, data_node_id, sql_db_path, setup_file_opcua)
            case 3:
                print('Multiple setup files found and no file specified.')    
            case _:
                print('Case mismatch')

    except Exception as e:
        print(e)
        logging.error(f"Main script error: {e}", exc_info=True)

# case for deciding which initialization function to run, depending on the setup file
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

# initialization for step7/snap7
def initialization_step7():
    try:
        # read the current setup file, and update previous_setup_file
        global previous_setup_file
        setup = read_setup_file(setup_file_step7)
        previous_setup_file = setup

        table_name = get_plc_from_file(setup_file_step7).get('table name') # get table name from setup file

        # set up sql database manager
        db_manager.sql_db_path = 'projekttestDB.db'
        db_manager.setup_file = setup_file_step7 
        db_manager.table_name = table_name

        if table_name is None:
            raise TableNotFoundError(f"Table name not found in setup file: {db_manager.setup_file}")

        db_manager.setup_sql_table_from_json() # set up the sql table with the manager
        return

    except TableNotFoundError as e:
        logging.error(e)

# initialization for opcua
def initialization_opcua():
    return

# reinitialization of sql database and settings from setup file
def reinitialize_setup():
    try: 
        # delete table data, then delete the table
        db_manager.delete_table_data() 
        db_manager.drop_table()

        init() # start initialization again
        return  

    except Exception as e:
        print(e)
        logging.error(f"Reinitialize setup error: {e}", exc_info=True)     

# ------------------------------------------------------ INITIALIZATION AND MAIN FUNCTION CALLS ------------------------------------------------------
start_init()
if __name__ == '__main__':
    start_main()


#------------------------------------------------------ FUNCTION CALLS FOR TESTING PURPOSES ------------------------------------------------------
#write_data_dbresult(db_manager)

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

#setup_sql_table_from_json(sql_db_path, 'Test_Table', setup_file_step7)
        
#print(setup_file_get_number_of_data_columns(setup_file_step7))
#print(read_setup_file(setup_file_step7))
#print(any_table_exists(sql_db_path))
        
#setup_file_rename_or_delete(setup_file_step7, 'column 4')        
#setup_file_rename_or_delete(setup_file_step7, 'column 5')   
        
#setup_file_keys_changed(setup_file_step7, previous_setup_file)


