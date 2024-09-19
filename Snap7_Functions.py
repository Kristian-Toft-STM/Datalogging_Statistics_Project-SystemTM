import snap7
import time
from snap7 import util
from json_functions import *
from logger import logger

import logging
import datetime

# initialize snap7 client 
client = snap7.client.Client()

# connect snap7 client to tcp server via step7
def connect_snap7_client(setup_file_step7):
    try:    
        
        # set up client connection from setup file    
        plc = get_plc_from_file(setup_file_step7)
        ip_address = plc.get('ip address')
        rack = plc.get('rack')
        slot = plc.get('slot')
        tcpport = plc.get('tcpport')

        client.set_param(snap7.types.RemotePort, tcpport)
        client.connect(ip_address, rack, slot)

        if client.get_connected():
            print("Snap7 client connected to tcp server.")
            return client
        else:
            print("Failed to connect client.")
            return None

    except Exception as e:
        print(e)
        logger.error(f"Connect snap7 client error: {e}", exc_info=True)

# disconnect snap7 client
def disconnect_snap7_client():
    try:     
        
        client.disconnect()
        print("Snap7 client disconnected from tcp server.")
        return
    
    except Exception as e:
        print(e)
        logger.error(f"Disconnect snap7 client error: {e}", exc_info=True)

# get single dint data from a given plc db and offset
def get_data_from_plc_db(db_number, client, index):
    try:
        if (client != None): # check if client is connected
            
            data = client.db_read(db_number, index, 4) # get data bytearray from plc
            data_fixed = util.get_dint(data,0) # convert bytearray to dint

        else:
            print("Client not connected in: get_data_from_plc_db")
            time.sleep(5)
            return
        
        return data_fixed

    except Exception as e: 
        print(e)
        logger.error(f"Get data from plc db error: {e}", exc_info=True)

# get array of dint data from a given plc db and offset
def get_data_array_from_plc_db(db_number, client, setup_file_step7): 
    try:

        statistics_insert_db_data_index = get_plc_from_file(setup_file_step7).get('insert_data_struct index') # get start index of data in dbinsert 
        data = client.db_read(db_number, statistics_insert_db_data_index, setup_file_get_number_of_data_columns(setup_file_step7)*4) # get data bytearray from plc, assuming all data is 4 bytes long
        array = [] # empty array for holding data
        
        # convert bytearray to array of dints
        for I in range(0, len(data), 4):
            if I + 4 <= len(data):   
                data_fixed = util.get_dint(data,I)    
                array.append(data_fixed) 
            else:
                break  

        return array

    except Exception as e:
        print(e)
        logger.error(f"Get data array from db error: {e}", exc_info=True)                

# monitor insert trigger and log data from plc on request
def monitor_and_insert_data_snap7(db_manager):               
    try:  
        oneshot = 0  
        if(oneshot == 0):
            client = connect_snap7_client(db_manager.setup_file)
            oneshot = 1
        
        if (client.get_connected() == False):  
            client = connect_snap7_client(db_manager.setup_file) # connect the snap 7 client

        if (client != None):

            trigger_value = 0 # initialise trigger value variable
            setup_file_step7 = db_manager.setup_file
            plc = get_plc_from_file(setup_file_step7)
            statistics_insert_db_number = plc.get('hs_statistics_db number')
            trigger_write_index = plc.get('trigger_write index')

            # int(2) = int value to convert to bytes - 4 is the length in bytes(int32 = 4 bytes) - byteorder is which direction you read the bytes
            bytearray_to_write = int(2).to_bytes(4, byteorder='big') # create variable containing the bytearray of an integer with value "2", for handshake with plc
            time.sleep(1)    
            while trigger_value == 0: # wait for trigger ready from plc
                try:
                    print(".")
                    trigger_value = get_data_from_plc_db(statistics_insert_db_number, client, trigger_write_index) # get trigger state from plc
                    if trigger_value == 1: # check if trigger ready from plc   
                        data_array = get_data_array_from_plc_db(statistics_insert_db_number, client, setup_file_step7) # get dbinsert data 
                        if data_array is not None: # check if data_array has data  
                            db_manager.insert_data_into_table(data_array) # insert data from dbinsert into the sql database table 
                        else: # if no data: skip loop iteration
                            continue      
                        client.db_write(statistics_insert_db_number, trigger_write_index, bytearray_to_write) # update logging trigger to signal python script done writing 
                        print("Logging triggered from PLC")
                        
                        return data_array  
                    time.sleep(0.5)                
                except Exception as e:
                    print(e)
                    logger.error(f"Monitor and insert data snap7 error: {e}", exc_info=True) 
                    time.sleep(2)  # Wait before attempting to reconnect
                    client = connect_snap7_client(setup_file_step7) #Re-establish connection  
            
        else:
            time.sleep(2)
            client = connect_snap7_client(db_manager.setup_file) #Re-establish connection 

        #column_names = setup_get_sql_column_names_from_file(db_manager.setup_file) # get list of columns names from setup file, for terminal print purposes
        #combined_array = [f"{value}: {data_item}" for data_item, column_dict in zip(data_array, column_names[1:]) for value in column_dict.items()] # create array of data and their respective column names
        #print("\n".join(combined_array))   
            
    except Exception as e:
        print(e)
        logger.error(f"Monitor and insert data snap7 error: {e}", exc_info=True)    
    finally:
        disconnect_snap7_client()

# monitor write trigger and get data from sql database on request 
def write_data_dbresult(db_manager, datetime_end=datetime.datetime.now()): 
    try:
        client = connect_snap7_client(db_manager.setup_file)
        plc = get_plc_from_file(db_manager.setup_file)         
            
        # get db numbers and indexes from setup file    
        statistics_request_db_number = plc.get('hs_statistics_db number')
        trigger_read_index = plc.get('trigger_read index')
        time_start_index = plc.get('time_start index')
        time_end_index = plc.get('time_end index')

        # initialise trigger variables
        bytearray_to_trigger = int(2).to_bytes(4, byteorder='big') 
        trigger_value = 0

        time.sleep(0.2)

        while True: # start loop to check for trigger (main loop)
            try:
                if (client.get_connected() == True):
                    datetime_end=datetime.datetime.now()
                    trigger_value = get_data_from_plc_db(statistics_request_db_number, client, trigger_read_index) 
                    if trigger_value == 1: # check for plc requesting data
                        
                        # check if time_start is 0 - proper error handling pls
                        if get_and_format_dtl_bytearray(statistics_request_db_number, time_start_index) == 0:
                            print('time_start = zero, where do i even begin?...')
                            client.db_write(statistics_request_db_number, trigger_read_index, bytearray_to_trigger)
                            continue

                        # get dtl range of logs 
                        start_dtl_datetime = get_and_format_dtl_bytearray(statistics_request_db_number, time_start_index)
                        print(f'start: {start_dtl_datetime}')
                        logger.info(f'start: {start_dtl_datetime}')
                        
                        # check if time_end is 0 - proper error handling pls
                        if get_and_format_dtl_bytearray(statistics_request_db_number, time_end_index) == 0:
                            print('time_end = zero, no end in sight...')
                            client.db_write(statistics_request_db_number, trigger_read_index, bytearray_to_trigger)
                            continue

                        end_dtl_datetime = get_and_format_dtl_bytearray(statistics_request_db_number, time_end_index)
                        print(f'end: {end_dtl_datetime}')
                        logger.info(f'end: {end_dtl_datetime}')
                        
                        # check if end dtl is defined, and if it is, use it to get logs. Below 1971 means it has the default value, and therefore has not been defined
                        if end_dtl_datetime.year > 1971:  
                            datetime_end = end_dtl_datetime  

                        data = db_manager.get_log_data_within_range(start_dtl_datetime, datetime_end) # get log data within start and end dtl
                        print(f'Data: {data}')
                        logger.info(f'Data: {data}')    

                        # check if any data was found within range, and if not, reset loop
                        if len(data) < 1:
                            print('No data found in supplied timestamp range') # proper error handling pls
                            logger.info('No data found in supplied timestamp range')
                            client.db_write(statistics_request_db_number, trigger_read_index, bytearray_to_trigger)
                            continue

                        if type(data) == list: 
                            time_sec = db_manager.get_seconds_in_range(start_dtl_datetime, datetime_end)
                            bytearray_to_data = bytearray()
                            for num in data:
                                bytearray_to_data += num.to_bytes(4, byteorder='big')
                            bytearray_to_time_sec = time_sec.to_bytes(4, byteorder='big')
                        else:
                            print('write_data_dbresult: unsupported datatype') # proper error handling pls
                            logger.info('write_data_dbresult: unsupported datatype')
                            client.db_write(statistics_request_db_number, trigger_read_index, bytearray_to_trigger)
                            continue

                        print(f'Seconds for range: {time_sec}')
                        logger.info(f'Seconds for range: {time_sec}')
                        client.db_write(statistics_request_db_number, plc.get('timespan_result index'), bytearray_to_time_sec) # write timespan to plc 
                        if bytearray_to_data != bytearray(b''): # check if bytearray is empty - proper error handling pls
                            client.db_write(statistics_request_db_number, plc.get('request_data_struct index'), bytearray_to_data) # write data to plc if bytearray not empty
                        client.db_write(statistics_request_db_number, trigger_read_index, bytearray_to_trigger) # update logging triger to signal python script done writing 

                    time.sleep(0.1)           
                       
                else:
                    time.sleep(2)
                    client = connect_snap7_client(db_manager.setup_file) #Re-establish connection        
                    

            except Exception as e:
                    print(e)
                    logger.error(f'Write data dbresult error: {e}', exc_info=True)
                    time.sleep(2)  # Wait before attempting to reconnect
                    client = connect_snap7_client(db_manager.setup_file) #Re-establish connection        
                
    finally:
        disconnect_snap7_client()   

# format dtl from plc to : yyyy-MM-dd-hh-mm-ss
def get_and_format_dtl_bytearray(db_number, index): 
    dtl_bytearray = client.db_read(db_number, index, 12) # get target dtl as a bytearray
    dtl_array = [] # array for holding converted dtl
    
    # loop through bytearray, converting each byte to an element in the array              
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
   
    year, month, day, hour, minute, second = dtl_array[:6] # map each element of the dtl array to a corresponding variable
    
    if year == 0 : # proper error handling pls
        return 0  
    
    dtl_datetime = datetime.datetime(year, month, day, hour, minute, second) # create a new datetime object with each of the date and time variables    
    return dtl_datetime
