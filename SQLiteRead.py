import sqlite3

def get_data_from_table(db_path, table):

    conn = sqlite3.connect(f'{db_path}')
    cursor = conn.cursor()

    cursor.execute(f"SELECT * FROM {table}")

    rows = cursor.fetchall() 
    rowsUnTupled = untuple(rows)

    for row in rowsUnTupled:
        print(row)

    cursor.close()
    conn.close()

    return rowsUnTupled

def untuple(array):
    I = 0
    unTupledArray = []
    for tup in array:
        unTupledArray.append(tup[0])
        I+=1
    return unTupledArray   
