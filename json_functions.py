import json
import logging

def read_setup_file(setup_file_name):
    try: 

        setup_file = open(f'{setup_file_name}')
        setup = json.load(setup_file)
        
        return setup

    except Exception as e:
        print(e)
        logging.error(f"Read setup file error: {e}", exc_info=True)   


def get_plc_from_file(setup_file_name):
    try:

        setup = read_setup_file(setup_file_name)
        plc1 = setup.get('PLC_1')
        return plc1[0]

    except Exception as e:
        print(e)
        logging.error(f"Get plc from file error: {e}", exc_info=True)       


def get_dbinsert_number_from_file(setup_file_name):
    try: 
    
        plc1 = get_plc_from_file(setup_file_name)
        dbinsert_number = plc1.get('dbinsert db number')
        return dbinsert_number

    except Exception as e:
        print(e)
        logging.error(f"Get dbinsert number from file error: {e}", exc_info=True)        


def get_dbinsert_logging_trigger_index_from_file(setup_file_name):
    try:
        plc1 = get_plc_from_file(setup_file_name)
        index = plc1.get('dbinsert_logging_trigger index')
        return index

    except Exception as e:
        print(e)
        logging.error(f"Get dbinsert logging trigger index from file error: {e}", exc_info=True)          


def get_dbinsert_data_index_from_file(setup_file_name):
    try:

        plc1 = get_plc_from_file(setup_file_name)
        index = plc1.get('dbinsert data index')
        return index

    except Exception as e:
        print(e)
        logging.error(f"Get dbinsert data index from file error: {e}", exc_info=True)  


def get_ip_from_file(setup_file_name):                        
    try:
        
        plc1 = get_plc_from_file(setup_file_name)
        ip = plc1.get('ip address')
        return ip

    except Exception as e:
        print(e)
        logging.error(f"Get ip from file error: {e}", exc_info=True)  


def setup_get_sql_column_names_from_file(setup_file_name):
    try:
            
        plc1 = get_plc_from_file(setup_file_name)
        columns = plc1.get('column names')
        
        return columns

    except Exception as e:
        print(e)
        logging.error(f"Setup get sql column names from file error: {e}", exc_info=True)  


def setup_file_column_names_dict_to_array(dict_columns):        
    try: 

        value_array = []  
        for column in dict_columns:  
            value_array.extend(column[key] for key in column)

        return value_array 
    
    except Exception as e:
        print(e)
        logging.error(f"Setup file column names dict to array error: {e}", exc_info=True)  


def map_node(key): #Start with mapping a single node, then expand to map all from file ""unfinished, continue later""
    try:

        setup = read_setup_file()
        node = setup[f'{key}']
        return node

    except Exception as e:
        print(e)
        logging.error(f"Map node error: {e}", exc_info=True)    


def setup_file_keys_changed(setup_file_name, previous_setup_file):
    try:

        previous_keys = set()
        current_keys = set()

        # Get all keys from the previous setup
        for plc_setup in previous_setup_file.values():
            for plc_info in plc_setup:
                previous_keys.update(plc_info.keys())

        # Get all keys from the current setup
        for plc_setup in setup_file_name.values():
            for plc_info in plc_setup:
                current_keys.update(plc_info.keys())

        # Find the keys that have been added or removed
        added_keys = current_keys - previous_keys
        removed_keys = previous_keys - current_keys

        if added_keys:
            print("Added keys:", added_keys)
            
        if removed_keys:
            print("Removed keys:", removed_keys)
            
        if removed_keys or added_keys:
            return True
        
        return False
    
    except Exception as e:
        print(e)
        logging.error(f"Setup file keys changed error: {e}", exc_info=True)  
    

def setup_file_get_number_of_data_columns(setup_file_name):
    try:

        plc = get_plc_from_file(setup_file_name)
        column_names = plc.get('column names')
        return len(column_names[1:])
        
    except Exception as e:
        print(e)
        logging.error(f"Setup file get column names length error: {e}", exc_info=True)   


def setup_file_add_column(setup_file_name, new_key, new_key_value, position):
    try:
        with open(setup_file_name, 'r') as setup_file:
            setup = json.load(setup_file)

        columns = setup['PLC_1'][0]['column names']
        columns.insert(position, {new_key: new_key_value})

        with open(setup_file_name, 'w') as file:
            json.dump(setup, file, indent=4, separators=(',', ': '))

        return
    
    except Exception as e:
        print(e)
        logging.error(f"Setup file add column error: {e}", exc_info=True)  


def setup_file_rename_column(setup_file_name, key, key_value):
    try:

        with open(setup_file_name, 'r') as setup_file:
            setup = json.load(setup_file)
        
        # Directly navigate to the 'column_names' assuming the structure you've provided
        columns = setup['PLC_1'][0]['column names']
    
        # Update the value of the specified key
        for column in columns:
            if key in column:
                column[key] = key_value

        # Write the updated setup back to the file
        with open(setup_file_name, 'w') as file:
            json.dump(setup, file, indent=4, separators=(',', ': '))

        return 

    except Exception as e:
        print(e)
        logging.error(f"Setup file rename column error: {e}", exc_info=True)          


def setup_file_delete_column(setup_file_name, key):
    try:

        with open(setup_file_name, 'r') as setup_file:
            setup = json.load(setup_file)
        
        columns = setup['PLC_1'][0]['column names']
        updated_columns = []

        for column in columns:
            if key not in column:
                updated_columns.append(column)

        setup['PLC_1'][0]['column names'] = updated_columns

        with open(setup_file_name, 'w') as file:
            json.dump(setup, file, indent=4, separators=(',', ': '))

        return 

    except Exception as e:
        print(e)
        logging.error(f"Setup file delete column error: {e}", exc_info=True)   


def setup_file_delete_or_rename(setup_file_name, key):
    try:    
        
        column_names = setup_get_sql_column_names_from_file(setup_file_name)
        for column in column_names:
            if key in column:
                if column[key][:1] == '!': 
                    print('Make new column with new name and delete data')  
                else:
                    print('Rename column and keep data') 
                      
        return

    except Exception as e:
            print(e)
            logging.error(f"Setup file delete column error: {e}", exc_info=True)


def save_previous_setup_step7(previous_setup_step7, filename='previous_setup_step7.json'):
    try: 

        with open(filename, 'w') as file:
            json.dump(previous_setup_step7, file)

    except Exception as e:
            print(e)
            logging.error(f"Save previous setup step7 error: {e}", exc_info=True)   


def load_previous_setup_step7(filename='previous_setup_step7.json'):
    try:

        with open(filename, 'r') as file:
            previous_setup_step7 = json.load(file)
        return previous_setup_step7
    
    except Exception as e:
            print(e)
            logging.error(f"Load previous setup step7 error: {e}", exc_info=True)
            