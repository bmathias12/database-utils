import logging

import psycopg2
import pandas as pd

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)-15s -- %(levelname)s: %(message)s',
                    datefmt='%Y-%m-%d %I:%M:%S %p')

class Database:
    def __init__(self, database, user, password, host):
        self.conn = psycopg2.connect(
            database=database,
            user=user,
            password=password,
            host=host)
    
    def close(self):
        self.conn.close()

    def query(self, sql):
        
        logging.info('Creating cursor')
        cursor = self.conn.cursor()
        try:
            blocks = [c+';' for c in sql.split(';')][0:-1]
            for i, block in enumerate(blocks):
                logging.info('Executing query step {0} of {1}'
                             .format(i+1, len(blocks)))
                cursor.execute(block)
            
            logging.info('Fetching Data')
            data = cursor.fetchall()
            columns = [c[0] for c in cursor.description]
            
            logging.info('Returning as dataframe')
            return pd.DataFrame.from_records(data, columns=columns)
        finally:
            logging.info('Closing cursor')
            cursor.close()
