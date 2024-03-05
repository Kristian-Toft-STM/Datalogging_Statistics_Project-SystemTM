import snap7
import time
from snap7 import util

ip_address = "172.31.1.60"
client = snap7.client.Client()

def connect_snap7_client():
    try: 
         
            #client.connect(Address, rack, slot, tcpport)   
            client.connect(ip_address, 0, 1, 102)

            print("Snap7 client connected to tcp server.")

            return client
            
    
    except Exception as e:
            print("Connect Snap7 client error:", e)


def disconnect_snap7_client():
    try:     
        
        client.disconnect()
        return
    
    except Exception as e:
            print("Disconnect Snap7 client error:", e)


def get_data_from_db(db_number,client, index):
    try:

        data = client.db_read(db_number, index, index+4)
        data_fixed = util.get_dint(data,0)

        return data_fixed

    except Exception as e:
            print("Get data from db error:", e)   
            
def get_data_array_from_db(db_number,client):
    try:
        
        data = client.db_read(db_number, 4, 32)
        array = []
        print(data)
        
        for I in range(0, len(data), 4): 
            if I + 4 <= len(data):   
                data_fixed = util.get_dint(data,I)           
                array.append(data_fixed) 
            else:
                break  

        return array

    except Exception as e:
            print("Get data array from db error:", e)                
          
def monitor_and_get_data_on_trigger_snap7(db_number, client):
    trigger_value = 0 
    client.db_write(db_number, 0, int(2).to_bytes(4, byteorder='big'))
    try:
        while trigger_value == 0:
            try:
                trigger_value = get_data_from_db(db_number, client, 0)
                if trigger_value == 1: 
                    print("Logging triggered from PLC")
                    
                    data_array = get_data_array_from_db(db_number, client)

                    #int(2) = int value to convert to bytes - 4 is the length in bytes(int = 32 = 4 bytes) - byteorder is which direction you read the bytes
                    client.db_write(db_number, 0, int(2).to_bytes(4, byteorder='big'))

                    return data_array   
            except Exception as e:
                print("Monitor snap7 error:", e)
                time.sleep(10)  # Wait before attempting to reconnect
                client = connect_snap7_client() #Re-establish connection
    finally:
        disconnect_snap7_client()  

#client = snap7.client.Client(lib_location="/path/to/snap7.dll")  # If the `snap7.dll` file is in another location

#client
#snap7.client.Client object at 0x0000028B257128E0>

#data = client.db_read("DB number, start byte, amount of bytes to read")
#data = client.db_read(2, 0, 4)
#data
#bytearray(b"\x00\x00\x00\x00")
#data[3] = 0b00000001
#data
#bytearray(b'\x00\x00\x00\x01')
#client.db_write(1, 0, data)

