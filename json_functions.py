import json
import logging

# read a given setup file
def read_setup_file(setup_file_name):
    try: 

        setup_file = open(f'{setup_file_name}')
        setup = json.load(setup_file)
        
        return setup

    except Exception as e:
        print(e)
        logging.error(f"Read setup file error: {e}", exc_info=True)   

# get plc1 from setup file
def get_plc_from_file(setup_file_name):
    try:

        setup = read_setup_file(setup_file_name)
        plc1 = setup.get('PLC_1')
        return plc1[0]

    except Exception as e:
        print(e)
        logging.error(f"Get plc from file error: {e}", exc_info=True)            

# get list of column names from setup file, for naming columns in sql database 
def setup_get_sql_column_names_from_file(setup_file_name):
    try:
            
        plc1 = get_plc_from_file(setup_file_name)
        columns = plc1.get('column names')
        
        return columns

    except Exception as e:
        print(e)
        logging.error(f"Setup get sql column names from file error: {e}", exc_info=True)  

# convert a dict to array
def setup_file_column_names_dict_to_array(dict_columns):        
    try: 

        value_array = []  
        for column in dict_columns:  
            value_array.extend(column[key] for key in column)

        return value_array 
    
    except Exception as e:
        print(e)
        logging.error(f"Setup file column names dict to array error: {e}", exc_info=True)  

# unused
def map_node(key): #Start with mapping a single node, then expand to map all from file ""unfinished, continue later""
    try:

        setup = read_setup_file()
        node = setup[f'{key}']
        return node

    except Exception as e:
        print(e)
        logging.error(f"Map node error: {e}", exc_info=True)    

# unused
def setup_file_keys_changed(setup_file_name, previous_setup_file): # not in use, possibly delete later
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
    
# gets the number of data columns in the setup file
def setup_file_get_number_of_data_columns(setup_file_name):
    try:

        plc = get_plc_from_file(setup_file_name)
        column_names = plc.get('column names')
        return len(column_names[1:])
        
    except Exception as e:
        print(e)
        logging.error(f"Setup file get column names length error: {e}", exc_info=True)   

# add a column to the setup file column names list
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

# rename a column in the setup file column names list
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

# delete a column from the setup file column names list
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

# given a key from column names in the setup files, read wether to delete or rename it depending on an identifier '!'
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

# save previous setup file step7
def save_previous_setup_step7(previous_setup_step7, filename='previous_setup_step7.json'):
    try: 

        with open(filename, 'w') as file:
            json.dump(previous_setup_step7, file)

    except Exception as e:
            print(e)
            logging.error(f"Save previous setup step7 error: {e}", exc_info=True)   

# load previous setup file step7
def load_previous_setup_step7(filename='previous_setup_step7.json'):
    try:

        with open(filename, 'r') as file:
            previous_setup_step7 = json.load(file)
        return previous_setup_step7
    
    except Exception as e:
            print(e)
            logging.error(f"Load previous setup step7 error: {e}", exc_info=True)
            
# using a txt file containing a list of names, insert these names into the column names in the setup file
def insert_list_of_column_names_from_txt_into_json(text_file, setup_file_path):
    try:
    
        name_array = []

        with open(text_file, 'r') as txtfile:
                line = txtfile.readline()
                while line:
                    name_array.append(line[:-1])
                    line = txtfile.readline()

        with open(setup_file_path, 'r') as setup_file:
            setup = json.load(setup_file)            
            column_names = setup['PLC_1'][0]['column names']  

        with open(setup_file_path, 'w') as setup_file:
            for I, column in enumerate(column_names[1:]):
                    key = f'column {I+2}'
                    column[key] = name_array[I]        
            json.dump(setup, setup_file, indent=4, separators=(',', ': '))         
    
        return name_array     
           
    except Exception as e:
            print(e)
            logging.error(f"insert list of column names from txt into json error: {e}", exc_info=True)