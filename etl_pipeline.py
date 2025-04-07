import pandas as pd
from sqlalchemy import create_engine, text


# Extract the data from .csv file
def extract_dat(file_path):
    try:
        df = pd.read_csv(file_path)
        print("✅ Data extraction successful!")
        return df
    except Exception as e:
        print(f'❌ Error in extracting data: {e}')
        return None


# Transform the data in the csv file
def transform_data(df):
    try:
        # Convert order_date to datetime
        df['order_date'] = pd.to_datetime(df['order_date'])

        #  Add a new column 'total_price'
        price_per_item = 5
        df['total_price'] = df['amount'] * price_per_item

        #  Clean any missing values (if any)
        df = df.dropna()

        print(f'✅ Data transformation successful!')
        return df
    except Exception as e:
        print(f'❌ Error in transforming data: {e}')
        return None


# Load the data into a Database
def load_data(df, database_name='customer_orders.db'):
    try:
        # Create SQLAlchemy engine to SQLite
        engine = create_engine(f'sqlite:///{database_name}')

        # Write data to a table called 'orders'
        df.to_sql('Orders', con=engine, if_exists='replace', index=False)

        print(f"✅ Data loaded successfully into {database_name}!")
    except Exception as e:
        print(f"❌ Error in loading data: {e}")


# Verify the data load
def query_data(database_name='customer_orders.db'):
    try:
        engine = create_engine(f'sqlite:///{database_name}')
        with engine.connect() as connection:
            result = connection.execute(text("SELECT * FROM orders LIMIT 5;"))
            print(f'✅ Sample data from the database:')
            for row in result:
                print(row)
    except Exception as e:
        print(f'❌ Error querying data: {e}')


if __name__ == "__main__":
    file_path = 'data/source_data.csv'
    data = extract_dat(file_path)
    print(data.head())

    if data is not None:
        transform_data = transform_data(data)
        print(transform_data.head())

        if transform_data is not None:
            load_data(transform_data)
            query_data()
