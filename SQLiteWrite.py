import sqlite3


def table_exists(db_path, table_name):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?",(table_name,))
    result = cursor.fetchone()

    cursor.close()
    conn.close()
    
    return result is not None

def DefineNewTable(db_path, Table, Text_Column1):
    try:
        print(f"db_path {db_path} - Table {Table} - Text_Column1 {Text_Column1}")
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        if not table_exists(db_path, Table):
            # If the table does not exist, create it
            print(f"Table {Table} does not exist. Creating it...")
            cursor.execute(f"CREATE TABLE {Table} ({Text_Column1} VARCHAR(50))")
            print(f"Table {Table} created.")

        cursor.close()
        conn.close()
    except Exception as e:
        print("Error:", e)

def InsertDummyDataIntoTable(db_path, Table, Column, Data):
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        print(f"INSERT INTO {Table} ({Column}) VALUES ({Data})")
        cursor.execute(f"INSERT INTO {Table} ({Column}) VALUES ({Data})")

        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        print("Error:", e)
