from opcua import Client,ua
import time
import numpy as np
from SQLiteWrite import insert_data_into_table
import logging

def connect_opcua_client():
    url = "opc.tcp://172.31.1.60:4840"  # Siemens PLC OPC UA server address
    client = Client(url)
    
    try:

        client.connect()
        print("Client connected to OPC UA server.")
        return client  # Return the client object after successful connection
    
    except Exception as e:
        print(e)
        logging.error(f"Connect opcua client error: {e}", exc_info=True)      
    

def disconnect_opcua_client(client):
    try:

        client.disconnect()
        print("Client disconnected from OPC UA server.")

    except Exception as e:
        print(e)
        logging.error(f"Disconnect opcua client error: {e}", exc_info=True)       


def read_node_value(client, node_id):
    try:

        target_node = client.get_node(node_id)
        target_node_value = target_node.get_value()
        return target_node_value
    
    except Exception as e:
        print(e)
        logging.error(f"Read node value error: {e}", exc_info=True)    
        return None


def monitor_and_get_data_on_trigger_opcua(client, trigger_node_id, data_node_id):
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
                print(e)
                logging.error(f"Monitor opcua error: {e}", exc_info=True)    
                time.sleep(10)  # Wait before attempting to reconnect
                client = connect_opcua_client() #Re-establish connection
    finally:
        disconnect_opcua_client(client)  # Disconnect from the OPC UA server when the loop stops      


def monitor_and_insert_data_opcua(sql_db_path, plc_trigger_id, table_name, data_node_id, setup_file_opcua):        
    try:   
        
        monitor_count = 1
        while monitor_count <= 10:
            client = connect_opcua_client()

            data_array = monitor_and_get_data_on_trigger_opcua(client, plc_trigger_id, data_node_id)

            if data_array is not None:
                insert_data_into_table(sql_db_path, table_name, data_array, setup_file_opcua)   

            print(f"Monitor count: {monitor_count}")
            monitor_count += 1
            
    except Exception as e:
        print(e)
        logging.error(f"onitor and insert data opcua error: {e}", exc_info=True)               

