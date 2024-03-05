from SQLiteWrite import define_new_table, insert_data_into_table, delete_table_data, drop_table, setup_sql_table_from_json
from SQLiteRead import get_data_from_table, get_logs_within_range
from OPCUA_Functions import connect_opcua_client, disconnect_opcua_client, read_node_value, monitor_and_get_data_on_trigger
from Snap7_Functions import connect_snap7_client, disconnect_snap7_client, get_data_from_db, get_data_array_from_db, monitor_and_get_data_on_trigger_snap7
from opcua import ua
import datetime
import pytz

from tzlocal import get_localzone

#client = connect_client()
plc_trigger_id = "ns=4;i=3"
data_node_id = "ns=4;i=4"
db_path = "projekttestDB.db"  # Your SQL database path
test_table = "Test_Data_Table"
test_column_name = "Test_Data_Column"
local_tz = get_localzone()
dbinsert_number = 10
setup_file_opcua = "setup_opcua.json"
setup_file_step7 = "setup_step7.json"

test_min_range = datetime.datetime(2024,3,4,8,50,18).astimezone(local_tz)
test_max_range = datetime.datetime(2024,3,4,9,57,58).astimezone(local_tz)          

def main_script_opcua_start(plc_trigger_id, data_node_id, db_path, test_table, test_column_name):
    monitor_count = 1
    try:
        while monitor_count <= 10:
            client = connect_opcua_client()

            data_array = monitor_and_get_data_on_trigger(client, plc_trigger_id, data_node_id)
            data_array_length = len(data_array)

            if data_array is not None:
                define_new_table(db_path, test_table, test_column_name, data_array_length)
                insert_data_into_table(db_path, test_table, test_column_name, data_array)

            print(f"Monitor count: {monitor_count}")
            monitor_count += 1

    except Exception as e:
        print("Main script error: ", e)

def main_script_snap7_start(sql_db_path, snap7_db_number, test_table, test_column_name):
    monitor_count = 1
    try:
        while monitor_count <= 10:
            client = connect_snap7_client()

            data_array = monitor_and_get_data_on_trigger_snap7(snap7_db_number, client)
            data_array_length = len(data_array)

            if data_array is not None:
                define_new_table(sql_db_path, test_table, test_column_name, data_array_length)
                insert_data_into_table(sql_db_path, test_table, test_column_name, data_array)

            print(f"Monitor count: {monitor_count}")
            monitor_count += 1

    except Exception as e:
        print("Main script error: ", e)


#main_script_opcua_start(plc_trigger_id, data_node_id, db_path, test_table, test_column_name)
#main_script_snap7_start(db_path, dbinsert_number, test_table, test_column_name)

#delete_table_data(db_path, test_table) 
#drop_table(db_path, test_table)         
#print(get_logs_within_range(db_path, test_table, test_min_range, test_max_range))

#print(read_setup_file())
#print(map_node(0))

#client = connect_Snap7_client()
#print(get_data_from_db(dbinsert_number, client, 0))   
#print(get_data_array_from_db(dbinsert_number, client))        
#monitor_and_get_data_on_trigger_Snap7(dbinsert_number, client)

#map_sql_columns_step7(setup_file_step7)
setup_sql_table_from_json(db_path, test_table, setup_file_step7)


