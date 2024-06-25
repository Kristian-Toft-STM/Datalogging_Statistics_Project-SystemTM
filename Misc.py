from json_functions import *
from SQLiteWrite import *

import logging
import csv
import time

# export table data from sql to csv file
def export_sql_to_csv(db_manager):
    try:    

        table_data = db_manager.get_all_data_from_table()
        filename = 'raw_data.csv'

        # Writing to the CSV file
        with open(filename, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            for row in table_data:
                writer.writerow(row)
        return
    
    except Exception as e:
        print(e)
        logging.error(f"Export sql to csv error: {e}", exc_info=True)

# timer for executing export_sql_to_csv()   
def csv_export_timer(db_manager):
    try: 

        while True: # loop for continous counting
            start = time.time()
            # change '5' to actual time (probably 24 hours)
            while time.time() < start + 5: 
                print("Time elapsed:", time.time() - start)  
                time.sleep(1) 

            end = time.time()
            length = end - start 
            export_sql_to_csv(db_manager)
            print("It took", length, "seconds!")

    except Exception as e:
        print(e)
        logging.error(f"CSV export timer error: {e}", exc_info=True)    