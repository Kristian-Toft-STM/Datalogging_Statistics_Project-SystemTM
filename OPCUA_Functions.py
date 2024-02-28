from opcua import Client

def connect_client():
    url = "opc.tcp://172.31.1.60:4840"  # Siemens PLC OPC UA server address
    client = Client(url)
    
    try:
        client.connect()
        print("Client connected to OPC UA server.")
        return client  # Return the client object after successful connection
    except Exception as e:
        # If connection fails, raise the exception to be handled in the main script
        raise e
    
def disconnect_client(client):
    try:
        client.disconnect()
        print("Disconnected")
    except Exception as e:
        print("Error during disconnection:", e)   

def read_node_value(client, node_id):
    try:
        var_node = client.get_node(node_id)
        value = var_node.get_value()
        return value
    except Exception as e:
        print("Error reading node value:", e)
        return None
