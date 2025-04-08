import unittest
import pandas as pd
from etl_pipeline import extract_data, transform_data, load_data
from sqlalchemy import create_engine, text


class TestETLPipeline(unittest.TestCase):

    def test_extract_data(self):
        df = extract_data('data/source_data.csv')
        self.assertIsNotNone(df)
        self.assertIn('customer_id', df.columns)
        self.assertIn('order_id', df.columns)

    def test_transform_data(self):
        df = pd.read_csv('data/source_data.csv')
        transformed_df = transform_data(df)
        self.assertIsNotNone(transformed_df)
        self.assertIn('total_price', transformed_df.columns)
        self.assertTrue((transformed_df['amount'] > 0).all())  # amount must be positive

    def test_load_data(self):
        df = pd.read_csv('data/source_data.csv')
        transformed_df = transform_data(df)
        load_data(transformed_df)
        # Check if the database exists and the data is loaded (simple check for number of rows)
        engine = create_engine('sqlite:///customer_orders.db')
        with engine.connect() as connection:
            result = connection.execute(text("SELECT COUNT(*) FROM Orders;"))
            row_count = result.fetchone()[0]
            self.assertGreater(row_count, 0)


if __name__ == "__main__":
    unittest.main()

