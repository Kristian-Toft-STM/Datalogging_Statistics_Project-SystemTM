import snap7
import time
from snap7 import util
from SQLiteWrite import insert_data_into_table
from SQLiteRead import get_log_data_within_range, get_number_of_rows_in_range, get_seconds_in_range
from json_functions import get_plc_from_file, setup_file_get_number_of_data_columns
import logging

client = snap7.client.Client()

def connect_snap7_client(setup_file_step7): # overvej at lave json funktioner til rack, slot og tcpport
    try: 
        plc = get_plc_from_file(setup_file_step7)
        ip_address = plc.get('ip address')
        rack = plc.get('rack')
        slot = plc.get('slot')
        tcpport = plc.get('tcpport')

        client.connect(ip_address, rack, slot, tcpport)

        print("Snap7 client connected to tcp server.")
        return client
            
    except Exception as e:
        print(e)
        logging.error(f"Connect snap7 client error: {e}", exc_info=True)


def disconnect_snap7_client():
    try:     
        
        client.disconnect()
        return
    
    except Exception as e:
        print(e)
        logging.error(f"Disconnect snap7 client error: {e}", exc_info=True)


def get_data_from_plc_db(db_number, client, index):
    try:

        data = client.db_read(db_number, index, index+4)
        data_fixed = util.get_dint(data,0)

        return data_fixed

    except Exception as e: 
        print(e)
        logging.error(f"Get data from plc db error: {e}", exc_info=True)


def get_data_array_from_plc_db(db_number, client, setup_file_step7): 
    try:
        data_index = get_plc_from_file.get('dbinsert data index')
        data = client.db_read(db_number, data_index, setup_file_get_number_of_data_columns(setup_file_step7)*4)
        array = []
        
        for I in range(0, len(data), 4):
            if I + 4 <= len(data):   
                data_fixed = util.get_dint(data,I)    
                array.append(data_fixed) 
            else:
                break  

        return array

    except Exception as e:
        print(e)
        logging.error(f"Get data array from db error: {e}", exc_info=True)                


def monitor_and_get_data_on_trigger_snap7(client, setup_file_step7):
    trigger_value = 0 
    plc = get_plc_from_file(setup_file_step7)
    db_number = plc.get('dbinsert db number')
    logging_trigger_index = plc.get('dbinsert_logging_trigger index')

    # Set handshake trigger bit
    # int(2) = int value to convert to bytes - 4 is the length in bytes(int32 = 4 bytes) - byteorder is which direction you read the bytes
    bytearray_to_write = int(2).to_bytes(4, byteorder='big')
    client.db_write(db_number, logging_trigger_index, bytearray_to_write)
    while trigger_value == 0:
        try:

            trigger_value = get_data_from_plc_db(db_number, client, logging_trigger_index)
            if trigger_value == 1: 
                        
                data_array = get_data_array_from_plc_db(db_number, client, setup_file_step7)
                client.db_write(db_number, logging_trigger_index, bytearray_to_write)
                print("Logging triggered from PLC")

                return data_array  
                    
        except Exception as e:
            print(e)
            logging.error(f"Monitor snap7 error: {e}", exc_info=True) 
            time.sleep(10)  # Wait before attempting to reconnect
            client = connect_snap7_client(setup_file_step7) #Re-establish connection  


def monitor_and_insert_data_snap7(sql_db_path, table_name, setup_file_step7, test_min_range, test_max_range):               
    try:    

            client = connect_snap7_client(setup_file_step7)
            data_array = monitor_and_get_data_on_trigger_snap7(client, setup_file_step7)

            if data_array is not None:  
                insert_data_into_table(sql_db_path, table_name, data_array, setup_file_step7)  
                
            write_data_dbresult(setup_file_step7, sql_db_path, table_name, test_min_range, test_max_range)  

    except Exception as e:
        print(e)
        logging.error(f"Monitor and insert data snap7 error: {e}", exc_info=True)    


def write_data_dbresult(setup_file_name, sql_db_path, table_name, datetime_min_range, datetime_max_range):
    try:
        client = connect_snap7_client(setup_file_name)
        trigger_value = 0 
        plc = get_plc_from_file(setup_file_name)
        db_number = plc.get('dbresult db number')
        logging_trigger_index = plc.get('dbresult_logging_trigger index')
        bytearray_to_trigger = int(2).to_bytes(4, byteorder='big')

        data = get_log_data_within_range(sql_db_path, table_name, datetime_min_range, datetime_max_range)
        print(data)
        while trigger_value == 0:    
            try:
                trigger_value = get_data_from_plc_db(db_number, client, logging_trigger_index)
                if trigger_value == 1: 
                    if type(data) == int: 
                        bytearray_to_data = data.to_bytes(4, byteorder='big')
                    elif type(data) == list: 
                        time_sec = get_seconds_in_range(sql_db_path, table_name, datetime_min_range, datetime_max_range)
                        bytearray_to_data = bytearray()
                        for num in data:
                            bytearray_to_data += num.to_bytes(4, byteorder='big')
                        bytearray_to_time_sec = time_sec.to_bytes(4, byteorder='big')
                
                    else:
                        print('write_data_dbresult: unsupported datatype')
                        return     

                    print(time_sec)
                    client.db_write(db_number, plc.get('dbresult time_sec index'), bytearray_to_time_sec)    
                    client.db_write(db_number, plc.get('dbresult data index'), bytearray_to_data)
                    client.db_write(db_number, logging_trigger_index, bytearray_to_trigger)
                    return bytearray_to_data            

            except Exception as e:
                print(e)
                logging.error(f"Write data dbresult error: {e}", exc_info=True)         
    finally:
        disconnect_snap7_client()