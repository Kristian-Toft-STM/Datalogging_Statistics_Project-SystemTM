import json

def read_setup_file(setup_file_name):
    try: 

        setup_file = open(f'{setup_file_name}')
        setup = json.load(setup_file)
        
        return setup

    except Exception as e:
        print("Read setup file error: ", e)   

def get_plc_from_file(setup_file_name):
    try:

        setup = read_setup_file(setup_file_name)
        plc1 = setup.get('PLC_1')
        return plc1[0]

    except Exception as e:
        print("Get plc from file error: ", e)       

def get_dbinsert_number_from_file(setup_file_name):
    try: 
    
        plc1 = get_plc_from_file(setup_file_name)
        dbinsert_number = plc1.get('dbinsert db number')
        return dbinsert_number

    except Exception as e:
        print("Get dbinsert number from file error: ", e)        

def get_dbinsert_logging_trigger_index_from_file(setup_file_name):
    try:
        plc1 = get_plc_from_file(setup_file_name)
        index = plc1.get('dbinsert_logging_trigger index')
        return index

    except Exception as e:
        print("Get DBinsert logging trigger index from file error: ", e)          

def get_dbinsert_data_index_from_file(setup_file_name):
    try:

        plc1 = get_plc_from_file(setup_file_name)
        index = plc1.get('dbinsert data index')
        return index

    except Exception as e:
        print("Get dbinsert data index from file error: ", e)  

def get_ip_from_file(setup_file_name):                        
    try:
        
        plc1 = get_plc_from_file(setup_file_name)
        ip = plc1.get('ip address')
        return ip

    except Exception as e:
        print("Get ip from file error: ", e)  

def setup_get_sql_column_names_from_file(setup_file_name):
    try:
            
        plc1 = get_plc_from_file(setup_file_name)
        columns = plc1.get('column names')
        
        return columns

    except Exception as e:
        print("Setup get sql column names from file error: ", e)  

def setup_file_column_names_dict_to_array(dict_columns):        
    try: 

        value_array = []  
        for column in dict_columns:  
            value_array.extend(column[key] for key in column)

        return value_array 
    
    except Exception as e:
        print("Setup file column names dict to array error: ", e)  

def map_node(key): #Start with mapping a single node, then expand to map all from file ""unfinished, continue later""
    try:

        setup = read_setup_file()
        node = setup[f'{key}']
        return node

    except Exception as e:
        print("Map node error: ", e)    

def file_changed(setup_file_name, previous_setup_file):
    try:

        if setup_file_name != previous_setup_file:
            return True
        else:
            return False
        
    except Exception as e:
        print("File changed error: ", e)  
    

def setup_file_get_number_of_data_columns(setup_file_name):
    try:

        plc = get_plc_from_file(setup_file_name)
        column_names = plc.get('column names')
        return len(column_names[1:])
        
    except Exception as e:
        print("Setup file get column names length error: ", e)   

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
        print("Setup file add column error: ", e)  
    
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
        print("Setup file rename column error: ", e)          

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
        print("Setup file delete column error: ", e)    

#Incomplete. Delete if easier to integrate directly in sql functions           
"""
def map_sql_columns_step7(setup_file_step7): 
    try:

        columns = setup_get_sql_column_names_step7(setup_file_step7)
        return columns        

    except Exception as e:
        print("Map sql columns error: ", e)  
"""          