from opcua import Client,ua
from SQLiteWrite import define_new_table
import time
import numpy as np

def connect_opcua_client():
    url = "opc.tcp://172.31.1.60:4840"  # Siemens PLC OPC UA server address
    client = Client(url)
    
    try:

        client.connect()
        print("Client connected to OPC UA server.")
        return client  # Return the client object after successful connection
    
    except Exception as e:
        # If connection fails, raise the exception to be handled in the main script
        raise e
    
def disconnect_opcua_client(client):
    try:

        client.disconnect()
        print("Client disconnected from OPC UA server.")

    except Exception as e:
        print("Disconnect error:", e)   

def read_node_value(client, node_id):
    try:

        target_node = client.get_node(node_id)
        target_node_value = target_node.get_value()
        return target_node_value
    
    except Exception as e:
        print("Read node value error: ", e)
        return None

def monitor_and_get_data_on_trigger(client, trigger_node_id, data_node_id):
    trigger_value = 0 
    trigger_node = client.get_node(trigger_node_id)
    trigger_node.set_value(ua.DataValue(ua.Variant(2, ua.VariantType.Int32))) 
    try:
        while trigger_value == 0:
            try:
                trigger_value = read_node_value(client, trigger_node_id)
                if trigger_value == 1: 
                    print("Logging triggered from PLC")

                    
                    data_array = read_node_value(client, data_node_id)
                    trigger_node.set_value(ua.DataValue(ua.Variant(2, ua.VariantType.Int32)))  

                    return data_array   
            except Exception as e:
                print("Monitor error:", e)
                time.sleep(10)  # Wait before attempting to reconnect
                client = connect_opcua_client() #Re-establish connection
    finally:
        disconnect_opcua_client(client)  # Disconnect from the OPC UA server when the loop stops        

def test():
    try:
        client = connect_opcua_client()
        
        root = client.get_root_node()
        #node_3 = root.get_child(["0:Objects","3:ServerInterfaces"])
        
        #node_4_motor = root.get_child(["0:Objects","3:ServerInterfaces","4:Server interface_1","4:DB_OPC","4:Motor"])
        node_4_motor = client.get_node("ns=4;i=4")
        node_4_motor_value = node_4_motor.get_value()
        
        #node_4_motor.ServerTimestamp = datetime.datetime.now()

        #print(node_4_motor)
        print(node_4_motor_value)

        #dv = ua.DataValue(ua.Variant(True, ua.VariantType.Boolean))
        #node_4_motor.set_value(dv)
        
        if node_4_motor_value == False:
            node_4_motor.set_attribute(ua.AttributeIds.Value, ua.DataValue(True))
        else:
            node_4_motor.set_attribute(ua.AttributeIds.Value, ua.DataValue(False))    

        node_4_motor_value = node_4_motor.get_value()
        print(node_4_motor_value)

        disconnect_opcua_client(client)

    except Exception as e:
        print("Error:", e)