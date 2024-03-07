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
        dbinsert_number = plc1.get('DBinsert DB Number')
        return dbinsert_number

    except Exception as e:
        print("Get dbinsert number from file error: ", e)        

def get_dbinsert_logging_trigger_index_from_file(setup_file_name):
    try:
        plc1 = get_plc_from_file(setup_file_name)
        index = plc1.get('DBinsert_Logging_Trigger Index')
        return index

    except Exception as e:
        print("Get DBinsert logging trigger index from file error: ", e)          

def get_data_index_from_file(setup_file_name):
    try:

        plc1 = get_plc_from_file(setup_file_name)
        index = plc1.get('Data Index')
        return index

    except Exception as e:
        print("Get data index from file error: ", e)  

def get_ip_from_file(setup_file_name):                        
    try:
        
        plc1 = get_plc_from_file(setup_file_name)
        ip = plc1.get('IP Address')
        return ip

    except Exception as e:
        print("Get ip from file error: ", e)  

def setup_get_sql_column_names_from_file(setup_file_name):
    try:
            
        plc1 = get_plc_from_file(setup_file_name)
        columns = plc1.get('column_names')
        
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

def file_changed(setup_file_name, previous_setup_file_name):
    return


#Incomplete. Delete if easier to integrate directly in sql functions           
"""
def map_sql_columns_step7(setup_file_step7): 
    try:

        columns = setup_get_sql_column_names_step7(setup_file_step7)
        return columns        

    except Exception as e:
        print("Map sql columns error: ", e)  
"""          