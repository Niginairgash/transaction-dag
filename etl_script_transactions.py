import pandas as pd
from sqlalchemy import create_engine
import logging

logging.basicConfig(
    filename='etl-pipline.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# extract data
def extract_data(file_path):
    logging.info(f"Extract datas from {file_path}")
    try:
        data = pd.read_csv(file_path)
        logging.info(f"Datas have successfully extract:{len(data)} lines")
    except Exception as e:
        logging.error(f"Error when extracting data: {e}")
        raise
    return data

# transform data
def transform_data(data):
    #data['transaction_date'] = pd.to_datetime(data['transaction_date'], format='YYYY-mm-dd')

    conversion_rates = {'USD': 1.0, 'EUR':1.2}

    data['amount_in_usd'] = data.apply(
        lambda row: row['amount'] * conversion_rates.get(row['currency'], 1.0), axis=1
    )

    data = data.dropna()

    return data

# load data
def load_data_to_db(data, db_connection_string):
    logging.info("Start loading data into the database")
    try:
        # create connection to db
        engine = create_engine(db_connection_string)
        
        # load data to table transactions
        data.to_sql('transactions', engine, index=False, if_exists='append')
        logging.info("Data successfully extract to db")
    except Exception as e:
        logging.error(f"Error loading data into database")
        raise
