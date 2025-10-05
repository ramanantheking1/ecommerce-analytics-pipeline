# etl_pipeline.py - FIXED ETL PIPELINE
import pandas as pd
from sqlalchemy import create_engine, text
import numpy as np
from datetime import datetime, timedelta

# DATABASE CONNECTIONS
source_engine = create_engine('mysql+pymysql://root:@localhost/ecommerce_source')
dw_engine = create_engine('mysql+pymysql://root:@localhost/ecommerce_dw')

def extract_data():
    """EXTRACT data from source database"""
    print("EXTRACTING data from source database...")
    
    try:
        customers_df = pd.read_sql("SELECT customer_id, first_name, last_name, email, city, registration_date FROM customers", source_engine)
        products_df = pd.read_sql("SELECT product_id, product_name, category, price FROM products", source_engine)
        orders_df = pd.read_sql("SELECT order_id, customer_id, order_date, total_amount, status FROM orders", source_engine)
        order_items_df = pd.read_sql("SELECT item_id, order_id, product_id, quantity, unit_price FROM order_items", source_engine)
        
        print(f" EXTRACTED: {len(customers_df)} customers, {len(products_df)} products, "
              f"{len(orders_df)} orders, {len(order_items_df)} order items")
        return customers_df, products_df, orders_df, order_items_df
        
    except Exception as e:
        print(f" EXTRACTION FAILED: {e}")
        return None, None, None, None

def transform_data(customers_df, products_df, orders_df, order_items_df):
    """TRANSFORM data: Clean, calculate, enrich"""
    print(" TRANSFORMING data with business logic...")
    
    try:
        # Merge orders with order_items
        sales_detail_df = pd.merge(
            orders_df[['order_id', 'customer_id', 'order_date', 'total_amount', 'status']], 
            order_items_df[['item_id', 'order_id', 'product_id', 'quantity', 'unit_price']], 
            on='order_id', 
            how='inner'
        )
        
        # Merge with products
        sales_detail_df = pd.merge(
            sales_detail_df,
            products_df[['product_id', 'product_name', 'category', 'price']],
            on='product_id',
            how='left'
        )
        
        # Merge with customers
        sales_detail_df = pd.merge(
            sales_detail_df,
            customers_df[['customer_id', 'first_name', 'last_name', 'city', 'registration_date']],
            on='customer_id', 
            how='left'
        )
        
        # Calculate new columns
        sales_detail_df['line_total'] = sales_detail_df['quantity'] * sales_detail_df['unit_price']
        sales_detail_df['profit'] = sales_detail_df['line_total'] * 0.3
        
        # Create customer segments
        customer_totals = orders_df.groupby('customer_id')['total_amount'].sum().reset_index()
        customer_totals['customer_segment'] = customer_totals['total_amount'].apply(
            lambda x: 'Premium' if x > 1000 else 'Gold' if x > 500 else 'Standard'
        )
        
        print(f" TRANSFORMED: Created enriched sales data with {len(sales_detail_df)} records")
        return sales_detail_df, customer_totals
        
    except Exception as e:
        print(f" TRANSFORMATION FAILED: {e}")
        return None, None

def create_data_warehouse_tables():
    """CREATE Data Warehouse tables (Star Schema)"""
    print(" CREATING Data Warehouse tables...")
    
    try:
        with dw_engine.connect() as conn:
            # Drop existing tables
            conn.execute(text("SET FOREIGN_KEY_CHECKS = 0"))
            tables = ['fact_sales', 'dim_customer', 'dim_product', 'dim_date']
            for table in tables:
                conn.execute(text(f"DROP TABLE IF EXISTS {table}"))
            conn.execute(text("SET FOREIGN_KEY_CHECKS = 1"))
            
            # Create dimension tables
            conn.execute(text("""
                CREATE TABLE dim_customer (
                    customer_key INT AUTO_INCREMENT PRIMARY KEY,
                    customer_id INT,
                    customer_name VARCHAR(100),
                    city VARCHAR(50),
                    customer_segment VARCHAR(20),
                    total_spent DECIMAL(10,2),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """))
            
            conn.execute(text("""
                CREATE TABLE dim_product (
                    product_key INT AUTO_INCREMENT PRIMARY KEY,
                    product_id INT,
                    product_name VARCHAR(100),
                    category VARCHAR(50),
                    price DECIMAL(10,2),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """))
            
            conn.execute(text("""
                CREATE TABLE dim_date (
                    date_key INT PRIMARY KEY,
                    full_date DATE,
                    day INT,
                    month INT, 
                    year INT,
                    quarter INT,
                    week INT,
                    day_name VARCHAR(10),
                    is_weekend BOOLEAN,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """))
            
            # Create fact table
            conn.execute(text("""
                CREATE TABLE fact_sales (
                    sales_key INT AUTO_INCREMENT PRIMARY KEY,
                    date_key INT,
                    customer_key INT,
                    product_key INT, 
                    order_id INT,
                    quantity INT,
                    amount DECIMAL(10,2),
                    profit DECIMAL(10,2),
                    line_total DECIMAL(10,2),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """))
            
        print(" Data Warehouse tables created successfully")
        return True
        
    except Exception as e:
        print(f" Data Warehouse creation failed: {e}")
        return False

def load_data_to_warehouse(sales_detail_df, customer_totals, products_df):
    """LOAD transformed data to Data Warehouse"""
    print(" LOADING data to Data Warehouse...")
    
    try:
        # Load dim_customer
        dim_customer_data = pd.merge(
            customer_totals,
            sales_detail_df[['customer_id', 'first_name', 'last_name', 'city']].drop_duplicates(),
            on='customer_id',
            how='left'
        )
        dim_customer_data['customer_name'] = dim_customer_data['first_name'] + ' ' + dim_customer_data['last_name']
        dim_customer_data = dim_customer_data[['customer_id', 'customer_name', 'city', 'customer_segment', 'total_amount']]
        dim_customer_data.rename(columns={'total_amount': 'total_spent'}, inplace=True)
        dim_customer_data.to_sql('dim_customer', dw_engine, if_exists='append', index=False)
        print("Loaded dim_customer table")
        
        # Load dim_product
        dim_product_data = products_df[['product_id', 'product_name', 'category', 'price']]
        dim_product_data.to_sql('dim_product', dw_engine, if_exists='append', index=False)
        print(" Loaded dim_product table")
        
        # Load dim_date
        unique_dates = sales_detail_df['order_date'].unique()
        date_data = []
        for date in unique_dates:
            date_key = int(date.strftime('%Y%m%d'))
            date_data.append({
                'date_key': date_key,
                'full_date': date,
                'day': date.day,
                'month': date.month,
                'year': date.year,
                'quarter': (date.month - 1) // 3 + 1,
                'week': date.isocalendar()[1],
                'day_name': date.strftime('%A'),
                'is_weekend': date.weekday() >= 5
            })
        dim_date_df = pd.DataFrame(date_data)
        dim_date_df.to_sql('dim_date', dw_engine, if_exists='append', index=False)
        print(" Loaded dim_date table")
        
        # Load fact_sales
        fact_sales_data = sales_detail_df.copy()
        fact_sales_data['date_key'] = fact_sales_data['order_date'].apply(lambda x: int(x.strftime('%Y%m%d')))
        fact_sales_data = fact_sales_data[['date_key', 'order_id', 'quantity', 'unit_price', 'profit', 'line_total']]
        fact_sales_data.rename(columns={'unit_price': 'amount'}, inplace=True)
        fact_sales_data.to_sql('fact_sales', dw_engine, if_exists='append', index=False)
        print(" Loaded fact_sales table")
        
        print(" DATA WAREHOUSE LOADING COMPLETED!")
        return True
        
    except Exception as e:
        print(f" LOADING FAILED: {e}")
        return False

def main():
    """MAIN function: Orchestrates the complete ETL process"""
    print(" STARTING COMPLETE ETL PIPELINE...")
    print("=" * 50)
    
    # STEP 1: EXTRACT
    customers_df, products_df, orders_df, order_items_df = extract_data()
    if customers_df is None:
        print(" ETL Pipeline failed at EXTRACT stage")
        return
    
    # STEP 2: TRANSFORM  
    sales_detail_df, customer_totals = transform_data(customers_df, products_df, orders_df, order_items_df)
    if sales_detail_df is None:
        print(" ETL Pipeline failed at TRANSFORM stage")
        return
    
    # STEP 3: CREATE DATA WAREHOUSE
    if not create_data_warehouse_tables():
        print(" ETL Pipeline failed at Data Warehouse creation")
        return
    
    # STEP 4: LOAD
    if not load_data_to_warehouse(sales_detail_df, customer_totals, products_df):
        print(" ETL Pipeline failed at LOAD stage")
        return
    
    print("=" * 50)
    print(" ETL PIPELINE COMPLETED SUCCESSFULLY!")
    print(" Your Data Warehouse is ready for analytics!")
    
    # Show summary
    with dw_engine.connect() as conn:
        dim_customer_count = pd.read_sql("SELECT COUNT(*) as count FROM dim_customer", conn).iloc[0]['count']
        fact_sales_count = pd.read_sql("SELECT COUNT(*) as count FROM fact_sales", conn).iloc[0]['count']
        
    print(f" Data Warehouse Summary:")
    print(f"   • dim_customer: {dim_customer_count} customers")
    print(f"   • fact_sales: {fact_sales_count} sales records")
    print(f"   • Ready for Power BI dashboards!")

if __name__ == "__main__":
    main()