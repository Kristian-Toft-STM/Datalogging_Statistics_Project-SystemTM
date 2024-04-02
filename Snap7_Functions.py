import snap7
import time
from snap7 import util
from json_functions import get_plc_from_file, setup_file_get_number_of_data_columns
import logging
import datetime

client = snap7.client.Client()

# connect snap7 client to tcp server via step7
def connect_snap7_client(setup_file_step7):
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

# disconnect snap7 client
def disconnect_snap7_client():
    try:     
        
        client.disconnect()
        return
    
    except Exception as e:
        print(e)
        logging.error(f"Disconnect snap7 client error: {e}", exc_info=True)

# get single dint data from a given plc db and offset
def get_data_from_plc_db(db_number, client, index):
    try:

        data = client.db_read(db_number, index, index+4)
        data_fixed = util.get_dint(data,0)

        return data_fixed

    except Exception as e: 
        print(e)
        logging.error(f"Get data from plc db error: {e}", exc_info=True)

# get array of dint data from a given plc db and offset
def get_data_array_from_plc_db(db_number, client, setup_file_step7): 
    try:

        data_index = get_plc_from_file(setup_file_step7).get('dbinsert data index')
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

# wait for trigger variable handshake from plc and get data on trigger, then finnish handshake by resetting trigger variable - snap7 
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

# bladibla
def monitor_and_insert_data_snap7(db_manager, test_min_range, test_max_range):               
    try:    

            client = connect_snap7_client(db_manager.setup_file)
            data_array = monitor_and_get_data_on_trigger_snap7(client, db_manager.setup_file)

            if data_array is not None:  
                db_manager.insert_data_into_table(data_array)  
                
            #write_data_dbresult(db_manager, test_min_range, test_max_range)  

    except Exception as e:
        print(e)
        logging.error(f"Monitor and insert data snap7 error: {e}", exc_info=True)    
    finally:
        disconnect_snap7_client()

# bladibla
# change how you get testtags_db_number and dtl_index
def write_data_dbresult(db_manager, datetime_min_range, datetime_max_range=datetime.datetime.now()):
    try:
        client = connect_snap7_client(db_manager.setup_file)
        trigger_value = 0 
        plc = get_plc_from_file(db_manager.setup_file)
        db_number = plc.get('dbresult db number')
        logging_trigger_index = plc.get('dbresult_logging_trigger index')
        bytearray_to_trigger = int(2).to_bytes(4, byteorder='big')
        testtags_db_number = 1001
        dtl_index = 950

        
        while trigger_value == 0:    
            try:
                trigger_value = get_data_from_plc_db(db_number, client, logging_trigger_index)
                if trigger_value == 1: 
                    
                    start_dtl_datetime = get_and_format_dtl_bytearray(testtags_db_number, dtl_index)
                    print(start_dtl_datetime)
                    print(datetime_max_range)

                    data = db_manager.get_log_data_within_range(start_dtl_datetime, datetime_max_range)
                    print(data)

                    if type(data) == int: 
                        bytearray_to_data = data.to_bytes(4, byteorder='big')
                    elif type(data) == list: 
                        time_sec = db_manager.get_seconds_in_range(datetime_min_range, datetime_max_range)
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

# format dtl from plc to : yyyy-MM-dd-hh-mm-ss
def get_and_format_dtl_bytearray(db_number, index):
    dtl_bytearray = client.db_read(db_number, index, 12)
    dtl_array = []              
    for I in range(0, len(dtl_bytearray), 1):
        if I + 4 <= len(dtl_bytearray):
            if I == 3 or I == 7:
                continue   
            if I == 0:  
                data_fixed = util.get_int(dtl_bytearray,I)
            elif I == 8:  
                data_fixed = util.get_dint(dtl_bytearray,I)    
            else:          
                data_fixed = util.get_sint(dtl_bytearray,I+1)    
            dtl_array.append(data_fixed)     
        else:
            break  
    year, month, day, hour, minute, second = dtl_array[:6]
    dtl_datetime = datetime.datetime(year, month, day, hour, minute, second)
    return dtl_datetime

