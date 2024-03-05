import json

def read_setup_file(setup_file_name):
    try: 
        
        setup_file = open(f'{setup_file_name}')
        setup = json.load(setup_file)  
        return setup

    except Exception as e:
        print("Read setup file error: ", e)   

def setup_get_sql_column_names_step7(setup_file_step7):
    try:
            
        setup = read_setup_file(setup_file_step7)
        plc1 = setup.get('PLC_1')
        columns = plc1[0].get('column_names')
        
        return columns

    except Exception as e:
        print("Get sql columns error: ", e)  

def map_node(key): #Start with mapping a single node, then expand to map all from file ""unfinished, continue later""
    try:

        setup = read_setup_file()
        node = setup[f'{key}']
        return node

    except Exception as e:
        print("Map node error: ", e)       

def map_sql_columns_step7(setup_file_step7): #Incomplete. Delete if easier to integrate directly in sql functions 
    try:

        columns = setup_get_sql_column_names_step7(setup_file_step7)
        return columns        

    except Exception as e:
        print("Map sql columns error: ", e)    