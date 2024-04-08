import snap7
import time
from snap7 import util
from json_functions import *
from Snap7_Functions import *
import logging
import datetime

client = snap7.client.Client()

def write_data_dbresult_old(db_manager, datetime_min_range, datetime_max_range=datetime.datetime.now()):
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

def write_data_dbresult_old_new(db_manager, datetime_end=datetime.datetime.now()):
    try:
        plc = get_plc_from_file(db_manager.setup_file)
        client = connect_snap7_client(db_manager.setup_file)
        
        db_number = plc.get('dbresult db number')
        logging_trigger_index = plc.get('dbresult_logging_trigger index')

        testtags_db_number = plc.get('testtags db number')
        dtl_start_index = plc.get('dtl start index')
        dtl_end_index = plc.get('dtl end index')

        bytearray_to_trigger = int(2).to_bytes(4, byteorder='big')

        trigger_value = 0
        
        while trigger_value == 0:    
            try:
                trigger_value = get_data_from_plc_db(db_number, client, logging_trigger_index)
                if trigger_value == 1: 
                    
                    start_dtl_datetime = get_and_format_dtl_bytearray(testtags_db_number, dtl_start_index)
                    end_dtl_datetime = get_and_format_dtl_bytearray(testtags_db_number, dtl_end_index)

                    if end_dtl_datetime.year > 1971:  
                        datetime_end = end_dtl_datetime  

                    data = db_manager.get_log_data_within_range(start_dtl_datetime, datetime_end)
                    print(data)    

                    if len(data) < 1:
                       print('No data found in supplied timestamp range')
                       return   
                                
                    if type(data) == int: 
                        bytearray_to_data = data.to_bytes(4, byteorder='big')
                    elif type(data) == list: 
                        time_sec = db_manager.get_seconds_in_range(start_dtl_datetime, datetime_end)
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