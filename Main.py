import time, datetime
from SQLiteWrite import define_new_table, insert_data_into_table
from SQLiteRead import get_data_from_table
from OPCUA_Functions import connect_client, disconnect_client, read_node_value, monitor_trigger_get_data
from opcua import ua

client = connect_client()
plc_trigger_id = "ns=4;i=3"
data_node_id = "ns=4;i=4"
db_path = "projekttestDB.db"  # Your SQL database path
test_table = "Test_Data_Table"
test_column_name = "Test_Data_Column"


data_array = monitor_trigger_get_data(client, plc_trigger_id, data_node_id)
data_array_length = len(data_array)
print(data_array_length)

if data_array is not None:
    define_new_table(db_path, test_table, test_column_name, data_array_length)
    insert_data_into_table(db_path, test_table, test_column_name, data_array)

    table_data_recieved = get_data_from_table(db_path, test_table)
    print(table_data_recieved)

