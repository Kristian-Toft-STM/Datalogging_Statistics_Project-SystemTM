from SQLiteWrite import define_new_table, insert_data_into_table, delete_table_data, drop_table
from SQLiteRead import get_data_from_table, get_logs_within_range
from OPCUA_Functions import connect_opcua_client, disconnect_opcua_client, read_node_value, monitor_and_get_data_on_trigger
from opcua import ua
import datetime
import pytz
import json
from tzlocal import get_localzone

#client = connect_client()
plc_trigger_id = "ns=4;i=3"
data_node_id = "ns=4;i=4"
db_path = "projekttestDB.db"  # Your SQL database path
test_table = "Test_Data_Table"
test_column_name = "Test_Data_Column"
local_tz = get_localzone()


test_min_range = datetime.datetime(2024,3,4,8,50,18).astimezone(local_tz)
test_max_range = datetime.datetime(2024,3,4,9,57,58).astimezone(local_tz)


def main_script_Start(plc_trigger_id, data_node_id, db_path, test_table, test_column_name):
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
        print("Main script error:", e)


#main_script_Start(plc_trigger_id, data_node_id, db_path, test_table, test_column_name)
#delete_table_data(db_path, test_table) 
#drop_table(db_path, test_table)         
#print(get_logs_within_range(db_path, test_table, test_min_range, test_max_range))

setup_file = open('setup.json')
setup = json.load(setup_file)  
for i in setup['PLC_1']:
    print(i)

    

