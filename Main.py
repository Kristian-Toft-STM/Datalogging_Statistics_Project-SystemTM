import time, datetime
from SQLiteWrite import DefineNewTable, InsertDummyDataIntoTable
from SQLiteRead import GetDataFromTable
from OPCUA_Functions import connect_client, disconnect_client, read_node_value
from opcua import ua

db_path = "projekttestDB.db"  # Your SQL database path

# DefineNewTable(db_path, "Test_Table", "Test_Column")
# InsertDummyDataIntoTable(db_path, "Test_Table", "Test_Column", "'Test_Data'")

# TableDataRecieved = GetDataFromTable(db_path, "Test_Table")
# print(TableDataRecieved)



def test():
    try:
        client = connect_client()
        
        root = client.get_root_node()
        #node_3 = root.get_child(["0:Objects","3:ServerInterfaces"])
        
        #node_4_motor = root.get_child(["0:Objects","3:ServerInterfaces","4:Server interface_1","4:DB_OPC","4:Motor"])
        node_4_motor = client.get_node("ns=4;i=4")
        node_4_motor_value = node_4_motor.get_value()
        
        #node_4_motor.ServerTimestamp = datetime.datetime.now()

        print(node_4_motor)
        print(node_4_motor_value)

        #dv = ua.DataValue(ua.Variant(True, ua.VariantType.Boolean))
        #node_4_motor.set_value(dv)
        if node_4_motor_value == False:
            node_4_motor.set_attribute(ua.AttributeIds.Value, ua.DataValue(True))
        else:
            node_4_motor.set_attribute(ua.AttributeIds.Value, ua.DataValue(False))    

        node_4_motor_value = node_4_motor.get_value()
        print(node_4_motor_value)

        disconnect_client(client)

    except Exception as e:
        print("Error:", e)
test()
