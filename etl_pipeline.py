import pandas as pd
from sqlalchemy import create_engine, text
import logging

# Set up a logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')



# Extract the data from .csv file
def extract_data(file_path, chunk_size=1000):
    try:
        chunk_list = []  # List to store chunks
        for chunk in pd.read_csv(file_path, chunksize=chunk_size):
            chunk_list.append(chunk)
        df = pd.concat(chunk_list, axis=0)
        logging.info("✅ Data extraction successful!")
        return df
    except Exception as e:
        logging.error(f"❌ Error in extracting data: {e}")
        return None


# Transform the data in the csv file
def transform_data(df):
    try:
        # Convert order_date to datetime
        df['order_date'] = pd.to_datetime(df['order_date'])


        # Check if amount is positive.
        if (df['amount'] <= 0).any():
            logging.warning("❌ Some rows have non-positive 'amount'.")


        # Check if order_date is not in the future
        if (df['order_date'] > pd.to_datetime('today')).any():
            logging.warning("❌ Some orders have future dates.")

        #  Add a new column 'total_price'
        price_per_item = 5
        df['total_price'] = df['amount'] * price_per_item

        #  Clean any missing values (if any)
        df = df.dropna()

        #  Remove duplicates
        df = df.drop_duplicates()

        #  Data aggragation: Group by 'customer_id' and sum the 'total_price'
        customer_spending = df.groupby('customer_id')['total_price'].sum().reset_index()
        customer_spending.rename(columns={'total_price': 'total_spent'}, inplace=True)
        df = pd.merge(df, customer_spending, on='customer_id', how='left')

        df['total_spent'] = df['total_spent'].fillna(0)  # Fill NaN with 0
        df['total_spent'] = df['total_spent'].astype(float)  # Ensure total_spent is float
        logging.info(f'✅ Data transformation successful!')
        return df
    except Exception as e:
        logging.info(f'❌ Error in transforming data: {e}')
        return None


# Load the data into a Database
def load_data(df, database_name='customer_orders.db'):
    try:
        # Create SQLAlchemy engine to SQLite
        engine = create_engine(f'sqlite:///{database_name}')

        # Write data to a table called 'orders'
        df.to_sql('Orders', con=engine, if_exists='replace', index=False)
        logging.info(f"✅ Data loaded successfully into {database_name}!")
    except Exception as e:
        logging.info(f"❌ Error in loading data: {e}")


# Verify the data load
def query_data(database_name='customer_orders.db'):
    try:
        engine = create_engine(f'sqlite:///{database_name}')
        with engine.connect() as connection:
            result = connection.execute(text("SELECT * FROM orders LIMIT 5;"))
            logging.info(f'✅ Sample data from the database:')
            for row in result:
                print(row)
    except Exception as e:
        logging.info(f'❌ Error querying data: {e}')


if __name__ == "__main__":
    file_path = 'data/source_data.csv'
    data = extract_data(file_path)
    logging.info(data.head())

    if data is not None:
        transform_data = transform_data(data)
        logging.info(transform_data.head())

        if transform_data is not None:
            load_data(transform_data)
            query_data()
