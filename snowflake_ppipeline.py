import snowflake.connector
import pandas as pd
from datetime import datetime

# Configuration for Snowflake connection
SNOWFLAKE_CONFIG = {
    'account': 'your_account',
    'user': 'your_user',
    'password': 'your_password',
    'warehouse': 'COMPUTE_WH',
    'database': 'SALES_DB',
    'schema': 'PUBLIC'
}

def connect_to_snowflake():
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    return conn

def load_csv_data(file_path):
    df = pd.read_csv(file_path)
    return df

def transform_data(df):
    # Basic transformation: convert date column to datetime and calculate total sales
    df['order_date'] = pd.to_datetime(df['order_date'])
    df['total_sales'] = df['quantity'] * df['unit_price']
    return df

def write_to_snowflake(conn, df, table_name):
    cursor = conn.cursor()
    # Create table if not exists
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            order_id STRING,
            order_date DATE,
            customer_id STRING,
            quantity INT,
            unit_price FLOAT,
            total_sales FLOAT
        )
    """)
    
    # Load data into Snowflake
    for _, row in df.iterrows():
        cursor.execute(f"""
            INSERT INTO {table_name} (order_id, order_date, customer_id, quantity, unit_price, total_sales)
            VALUES ('{row['order_id']}', '{row['order_date']}', '{row['customer_id']}', 
                    {row['quantity']}, {row['unit_price']}, {row['total_sales']})
        """)
    conn.commit()
    cursor.close()

def main():
    # Sample pipeline
    file_path = 'sales_data.csv'
    table_name = 'SALES_DATA'
    
    # Connect to Snowflake
    conn = connect_to_snowflake()
    
    # Load and transform data
    df = load_csv_data(file_path)
    transformed_df = transform_data(df)
    
    # Write to Snowflake
    write_to_snowflake(conn, transformed_df, table_name)
    
    conn.close()

if __name__ == "__main__":
    main()