import snap7
import time
from snap7 import util
from SQLiteWrite import insert_data_into_table
from json_functions import get_dbinsert_number_from_file, get_dbinsert_logging_trigger_index_from_file, get_data_index_from_file, get_ip_from_file, get_plc_from_file

client = snap7.client.Client()

def connect_snap7_client(setup_file_step7): # overvej at lave json funktioner til rack, slot og tcpport
    try: 
        ip_address = get_ip_from_file(setup_file_step7)
        #client.connect(Address, rack, slot, tcpport) 
        client.connect(ip_address, get_plc_from_file(setup_file_step7).get('rack'), get_plc_from_file(setup_file_step7).get('slot'), get_plc_from_file(setup_file_step7).get('tcpport'))

        print("Snap7 client connected to tcp server.")
        return client
            
    except Exception as e:
        print("Connect snap7 client error:", e)


def disconnect_snap7_client():
    try:     
        
        client.disconnect()
        return
    
    except Exception as e:
            print("Disconnect Snap7 client error:", e)

def get_data_from_plc_db(db_number,client, index):
    try:

        data = client.db_read(db_number, index, index+4)
        data_fixed = util.get_dint(data,0)

        return data_fixed

    except Exception as e:
            print("Get data from plc db error:", e)   
            
def get_data_array_from_plc_db(db_number, client, setup_file_step7):
    try:
        data_index = get_data_index_from_file(setup_file_step7)
        data = client.db_read(db_number, data_index, 32)
        array = []
        
        for I in range(0, len(data), data_index): 
            if I + 4 <= len(data):   
                data_fixed = util.get_dint(data,I)           
                array.append(data_fixed) 
            else:
                break  

        return array

    except Exception as e:
            print("Get data array from db error:", e)                

def monitor_and_get_data_on_trigger_snap7(client, setup_file_step7):
    trigger_value = 0 
    db_number = get_dbinsert_number_from_file(setup_file_step7)
    logging_trigger_index = get_dbinsert_logging_trigger_index_from_file(setup_file_step7)

    #int(2) = int value to convert to bytes - 4 is the length in bytes(int32 = 4 bytes) - byteorder is which direction you read the bytes
    bytearray_to_write = int(2).to_bytes(4, byteorder='big')
    client.db_write(db_number, logging_trigger_index, bytearray_to_write)
    try:

        while trigger_value == 0:
            try:

                trigger_value = get_data_from_plc_db(db_number, client, 0)
                if trigger_value == 1: 
                    
                    data_array = get_data_array_from_plc_db(db_number, client, setup_file_step7)
                    client.db_write(db_number, logging_trigger_index, bytearray_to_write)
                    print("Logging triggered from PLC")

                    return data_array  
                 
            except Exception as e:
                print("Monitor snap7 error:", e)
                time.sleep(10)  # Wait before attempting to reconnect
                client = connect_snap7_client(setup_file_step7) #Re-establish connection

    finally:
        disconnect_snap7_client()  


def monitor_and_insert_data_snap7(sql_db_path, table_name, setup_file_step7):        
    try:    

        monitor_count = 1
        while monitor_count <= 10:
            client = connect_snap7_client(setup_file_step7)
            data_array = monitor_and_get_data_on_trigger_snap7(client, setup_file_step7)

            if data_array is not None:  
                insert_data_into_table(sql_db_path, table_name, data_array, setup_file_step7)   

            print(f"Monitor count: {monitor_count}")
            monitor_count += 1

    except Exception as e:
            print("Monitor and insert data snap7 error:", e)   


def write_data_dbresult(setup_file_name, data):
    try:
        client = connect_snap7_client(setup_file_name)
        plc = get_plc_from_file(setup_file_name)
        if type(data) == int: 
            bytearray_to_write = data.to_bytes(4, byteorder='big')
        elif type(data) == list: 
            bytearray_to_write = bytearray()
            for num in data:
                bytearray_to_write += num.to_bytes(4, byteorder='big')   
        else:
             print('write_data_dbresult: unsupported datatype')
             return     
        client.db_write(plc.get("DBresult DB Number"), 0, bytearray_to_write)
        return bytearray_to_write            

    except Exception as e:
        print("Write data dbresult error:", e)       
    
