# ------------------------------------------------------ IMPORTS ------------------------------------------------------
# sub-script imports
#from OPCUA_Functions import *
from Snap7_Functions import *
from json_functions import *
from Misc import *
from test import *
from SQLiteRead import SQLDatabaseManager

# library imports
#from opcua import ua
import os
import logging
import inspect
from multiprocessing import *

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

# ------------------------------------------------------ TEST VARIABLES ------------------------------------------------------
plc_trigger_id = "ns=4;i=3"
data_node_id = "ns=4;i=4" 
#sql_db_path = "projekttestDB.db" lav i json.setup fil?
setup_file_opcua = "setup_opcua.json"
setup_file_step7 = "setup_step7.json"

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
        
        print("Initialization complete.")
        logging.info("Initialization complete.")
        return
    
    except Exception as e:
        print(f"Initialization error: {e}")
        logging.error(f"Initialization error: {e}", exc_info=True)

# start main functionality procs, running asynchronously to eachother
def start_main():
    try:
        main_proc = Process(target=main) # main functionality, including cyclically logging to sql database
        write_data_dbresult_proc = Process(target=write_data_dbresult, args=(db_manager,)) # monitor requests for writing data to plc
        #csv_export_timer_proc = Process(target=csv_export_timer, args=(sql_db_path, table_name)) # csv export timer

        main_proc.start()
        time.sleep(1) # hopefully this will fix connection issues to the plc
        write_data_dbresult_proc.start()
        #csv_export_timer_proc.start()

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
def main_loop_opcua_start(plc_trigger_id, data_node_id, sql_db_path, setup_file_opcua):
    try:
        
        table_name = db_manager.table_name
        db_manager.setup_sql_table_from_json()
        #monitor_and_insert_data_opcua(sql_db_path, plc_trigger_id, table_name, data_node_id, setup_file_opcua)

    except Exception as e:
        print(e)
        logging.error(f"Main opcua script error: {e}", exc_info=True)

# main script for step7/snap7 communication
def main_loop_snap7_start():
    try:
        # loop for continously monitor for logging requests, cycles limited by monitor_counter
        monitor_count = 1
        while True:
            monitor_and_insert_data_snap7(db_manager) # monitor for logging requests

            print(f"Monitor count: {monitor_count}")
            if monitor_count > 1000000:
                monitor_count = 0
            monitor_count += 1
            time.sleep(0.5)

    except Exception as e:
        print(e)
        logging.error(f"Main snap7 script error: {e}", exc_info=True)

# case for deciding which main script to run, depending on the setup file
def main():
    try: 
        match setup_file_to_run:
            case 0:
                print('No setup file found')
            case 1:
                print('Step 7 setup found, running it now...')
                main_loop_snap7_start()
            case 2:
                pass
                #print('Opcua setup found, running it now...')
                #main_loop_opcua_start(plc_trigger_id, data_node_id, sql_db_path, setup_file_opcua)
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
        db_manager.sql_db_path = 'opti-track.db'
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
    try:
        freeze_support()
        start_main()
    except Exception as e:
        print(f"Unhandled error: {e}")

#------------------------------------------------------ FUNCTION CALLS FOR TESTING PURPOSES ------------------------------------------------------



