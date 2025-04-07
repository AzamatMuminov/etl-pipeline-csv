# Customer Orders ETL Pipeline

This project is an end-to-end ETL (Extract, Transform, Load) pipeline that processes customer order data from a CSV file and loads it into an SQLite database.

## Features
- ✅ Extract data from CSV
- ✅ Transform and clean data (add `total_price`, format dates)
- ✅ Load data into SQLite database
- ✅ Verify data with sample SQL query

## Technologies Used
- Python
- Pandas
- SQLAlchemy
- SQLite

## How to Run

1. Install dependencies:
   pip install -r requirements.txt
2. Run the ETL pipeline:
   python .\etl_pipeline.py


## Database

The final data is stored in `customer_orders.db` in the table `orders`.

## Output

Sample output:
✅ Data extraction successful!
customer_id  order_id  order_date product  amount
0            1      1001  2024-04-01   Apple      10
1            2      1002  2024-04-01  Banana      20
2            1      1003  2024-04-02  Orange      15
3            3      1004  2024-04-03   Apple       5
✅ Data transformation successful!
customer_id  order_id order_date product  amount  total_price
0            1      1001 2024-04-01   Apple      10           50
1            2      1002 2024-04-01  Banana      20          100
2            1      1003 2024-04-02  Orange      15           75
3            3      1004 2024-04-03   Apple       5           25
✅ Data loaded successfully into customer_orders.db!
✅ Sample data from the database:
(1, 1001, '2024-04-01 00:00:00.000000', 'Apple', 10, 50)
(2, 1002, '2024-04-01 00:00:00.000000', 'Banana', 20, 100)
(1, 1003, '2024-04-02 00:00:00.000000', 'Orange', 15, 75)
(3, 1004, '2024-04-03 00:00:00.000000', 'Apple', 5, 25)

## Author

Azamat Muminov

