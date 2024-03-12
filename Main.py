from SQLiteWrite import insert_data_into_table, delete_table_data, drop_table, setup_sql_table_from_json, sql_rename_column, sql_drop_column, sql_add_column
from SQLiteRead import get_all_data_from_table, get_log_timestamps_within_range, get_log_data_within_range, get_log_data_within_range_sql_sum, any_table_exists
from OPCUA_Functions import connect_opcua_client, disconnect_opcua_client, read_node_value, monitor_and_get_data_on_trigger_opcua, monitor_and_insert_data_opcua
from Snap7_Functions import connect_snap7_client, disconnect_snap7_client, get_data_from_plc_db, get_data_array_from_plc_db, monitor_and_get_data_on_trigger_snap7, monitor_and_insert_data_snap7, write_data_dbresult
from json_functions import setup_get_sql_column_names_from_file, setup_file_column_names_dict_to_array, get_dbinsert_number_from_file, get_plc_from_file, setup_file_get_number_of_data_columns, read_setup_file, file_changed, setup_file_rename_column, setup_file_delete_column, setup_file_add_column
from opcua import ua
import datetime
import pytz
import os

from tzlocal import get_localzone

#client = connect_client()
plc_trigger_id = "ns=4;i=3"
data_node_id = "ns=4;i=4"
sql_db_path = "projekttestDB.db" 
setup_file_opcua = "setup_opcua.json"
setup_file_step7 = "setup_step7.json"

previous_setup_file = None

local_tz = get_localzone()
test_min_range = datetime.datetime(2024,3,12,13,0,0).astimezone(local_tz)
test_max_range = datetime.datetime(2024,3,12,23,0,0).astimezone(local_tz)          

def step7_or_opcua_switch(file_to_run):
    script_directory = os.path.dirname(__file__)
    directory_contents = os.listdir(script_directory)
    try:
        
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
        print("Step7 or opcua switch error: ", e)

def main_script_opcua_start(plc_trigger_id, data_node_id, sql_db_path, setup_file_opcua):
    try:
        
        table_name = get_plc_from_file(setup_file_opcua).get('table name')
        setup_sql_table_from_json(sql_db_path, table_name, setup_file_opcua)
        monitor_and_insert_data_opcua(sql_db_path, plc_trigger_id, table_name, data_node_id, setup_file_opcua)

    except Exception as e:
        print("Main opcua script error: ", e)

def main_script_snap7_start(sql_db_path, setup_file_step7):
    try:
        
        global previous_setup_file
        setup = read_setup_file(setup_file_step7)
        
        previous_setup_file = setup

        table_name = get_plc_from_file(setup_file_step7).get('table name')
        setup_sql_table_from_json(sql_db_path, table_name, setup_file_step7)
        monitor_and_insert_data_snap7(sql_db_path, table_name, setup_file_step7, test_min_range, test_max_range)

    except Exception as e:
        print("Main snap7 script error: ", e)

def main_script(file_to_run=''):
    try:    

        match step7_or_opcua_switch(file_to_run):
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
        print("Main script error: ", e)

def add_datapoint(setup_file_name, datapoint_key, datapoint_key_value, sql_db_path, position): # maybe make a new script for this and similar functions 
    try:
        
        table = get_plc_from_file(setup_file_step7).get('table name')

        setup_file_add_column(setup_file_name, datapoint_key, datapoint_key_value, position)
        sql_add_column(sql_db_path, table, datapoint_key_value)
        return

    except Exception as e:
        print("Add datapoint error: ", e)

def rename_datapoint(setup_file_name, datapoint_key, datapoint_key_value, sql_db_path): # maybe make a new script for this and similar functions 
    try:
        
        table = get_plc_from_file(setup_file_step7).get('table name')
        columns = get_plc_from_file(setup_file_step7).get('column names')

        for column_dict in columns:
            if datapoint_key in column_dict:
                column = column_dict[datapoint_key]

        setup_file_rename_column(setup_file_name, datapoint_key, datapoint_key_value)
        sql_rename_column(sql_db_path, table, column, datapoint_key_value)
        return

    except Exception as e:
        print("Rename datapoint error: ", e)

def delete_datapoint(setup_file_name, datapoint_key, sql_db_path): # maybe make a new script for this and similar functions 
    try:
        table = get_plc_from_file(setup_file_step7).get('table name')
        columns = get_plc_from_file(setup_file_step7).get('column names')

        for column_dict in columns:
            if datapoint_key in column_dict:
                column = column_dict[datapoint_key]

        setup_file_delete_column(setup_file_name, datapoint_key)
        sql_drop_column(sql_db_path, table, column)    
        return 
      
    except Exception as e:
        print("Delete datapoint error: ", e)


main_script(setup_file_step7)
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

#client = connect_Snap7_client()
#print(get_data_from_db(dbinsert_number, client, 0))   
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

#write_data_dbresult(setup_file_step7, sql_db_path, test_min_range, test_max_range)    
#setup_sql_table_from_json(sql_db_path, 'Test_Table', setup_file_step7)
        
#print(setup_file_get_number_of_data_columns(setup_file_step7))
#print(read_setup_file(setup_file_step7))
#print(any_table_exists(sql_db_path))
        
