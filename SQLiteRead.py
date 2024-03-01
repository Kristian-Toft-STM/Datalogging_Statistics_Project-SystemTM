import sqlite3

def get_data_from_table(db_path, table):

    conn = sqlite3.connect(f'{db_path}')
    cursor = conn.cursor()

    cursor.execute(f"SELECT * FROM {table}")

    rows = cursor.fetchall() 
    rowsUnTupled = untuple(rows)

    cursor.close()
    conn.close()

    return rowsUnTupled

def get_last_timestamp_from_table(db_path, table):
    conn = sqlite3.connect(f'{db_path}')
    cursor = conn.cursor()

    cursor.execute(f"SELECT * FROM {table} ORDER BY TimeStamp DESC LIMIT 1;")
    last_timestamp_row = cursor.fetchone()
    last_timestamp = last_timestamp_row[0]

    cursor.close()
    conn.close()

    return last_timestamp

def untuple(tuple):
    I = 0
    untupled_array = []
    for t in tuple:
        untupled_array.append(t[0])
        I+=1
    return untupled_array  
