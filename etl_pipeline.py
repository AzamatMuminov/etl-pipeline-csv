import pandas as pd
from sqlalchemy import create_engine

# Extract the data from .csv file
def extract_dat(file_path):
    try:
        df = pd.read_csv(file_path)
        print("✅ Data extraction successful!")
        return df
    except Exception as e:
        print(f'❌ Error in extracting data: {e}')
        return None

if __name__ == "__main__":
    file_path = 'data/source_data.csv'
    data = extract_dat(file_path)
    print(data.head())
