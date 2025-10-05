# generate_data.py - DEBUG VERSION
import pandas as pd
import random
from faker import Faker
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
import numpy as np
import sys

print(" DEBUG: Starting generate_data.py...")

# Initialize faker
fake = Faker()

try:
    # Database connection - TEST FIRST
    print(" Testing database connection...")
    engine = create_engine('mysql+pymysql://root:@localhost/ecommerce_source')
    
    # Test connection
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    print(" Database connection successful!")
    
except Exception as e:
    print(f" DATABASE CONNECTION FAILED: {e}")
    print(" TROUBLESHOOTING:")
    print("   1. Is MySQL running?")
    print("   2. Is the database 'ecommerce_source' created?")
    print("   3. Check username/password in connection string")
    sys.exit(1)

def clear_existing_data():
    """Clear existing data to avoid foreign key conflicts"""
    try:
        with engine.connect() as conn:
            # Disable foreign key checks
            conn.execute(text("SET FOREIGN_KEY_CHECKS = 0"))
            
            # Clear tables in correct order
            tables = ['order_items', 'orders', 'products', 'customers']
            for table in tables:
                conn.execute(text(f"DROP TABLE IF EXISTS {table}"))
                print(f" Dropped table {table}")
            
            # Re-enable foreign key checks
            conn.execute(text("SET FOREIGN_KEY_CHECKS = 1"))
            
        print(" Cleared existing data")
        return True
    except Exception as e:
        print(f" Error clearing data: {e}")
        return False

def create_tables():
    """Recreate the database tables"""
    try:
        with engine.connect() as conn:
            # Create customers table
            conn.execute(text("""
                CREATE TABLE customers (
                    customer_id INT PRIMARY KEY,
                    first_name VARCHAR(50),
                    last_name VARCHAR(50),
                    email VARCHAR(100),
                    city VARCHAR(50),
                    registration_date DATE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """))
            print(" Created customers table")
            
            # Create products table
            conn.execute(text("""
                CREATE TABLE products (
                    product_id INT PRIMARY KEY,
                    product_name VARCHAR(100),
                    category VARCHAR(50),
                    price DECIMAL(10,2),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """))
            print(" Created products table")
            
            # Create orders table
            conn.execute(text("""
                CREATE TABLE orders (
                    order_id INT PRIMARY KEY,
                    customer_id INT,
                    order_date DATE,
                    total_amount DECIMAL(10,2),
                    status VARCHAR(20),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
                )
            """))
            print(" Created orders table")
            
            # Create order_items table
            conn.execute(text("""
                CREATE TABLE order_items (
                    item_id INT PRIMARY KEY,
                    order_id INT,
                    product_id INT,
                    quantity INT,
                    unit_price DECIMAL(10,2),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (order_id) REFERENCES orders(order_id),
                    FOREIGN KEY (product_id) REFERENCES products(product_id)
                )
            """))
            print(" Created order_items table")
            
        print(" Database tables created successfully")
        return True
    except Exception as e:
        print(f" Error creating tables: {e}")
        return False

def generate_customers(num_customers=50):
    """Generate fake customer data"""
    customers = []
    for i in range(1, num_customers + 1):
        customers.append({
            'customer_id': i,
            'first_name': fake.first_name(),
            'last_name': fake.last_name(),
            'email': fake.email(),
            'city': fake.city(),
            'registration_date': fake.date_between(start_date='-2y', end_date='today')
        })
    return pd.DataFrame(customers)

def generate_products(num_products=20):
    """Generate fake product data"""
    categories = ['Electronics', 'Clothing', 'Books', 'Home & Kitchen', 'Sports', 'Beauty']
    products = []
    for i in range(1, num_products + 1):
        category = random.choice(categories)
        base_price = random.uniform(10, 500)
        products.append({
            'product_id': i,
            'product_name': f"{category} Product {i}",
            'category': category,
            'price': round(base_price, 2)
        })
    return pd.DataFrame(products)

def generate_orders(num_orders=100):
    """Generate fake order data"""
    orders = []
    order_items = []
    item_id = 1
    
    for order_num in range(1, num_orders + 1):
        customer_id = random.randint(1, 50)
        order_date = fake.date_between(start_date='-1y', end_date='today')
        
        # Create order
        orders.append({
            'order_id': order_num,
            'customer_id': customer_id,
            'order_date': order_date,
            'total_amount': 0,  # Will calculate after items
            'status': random.choice(['completed', 'pending', 'shipped'])
        })
        
        # Create 1-5 items per order
        num_items = random.randint(1, 5)
        order_total = 0
        
        for _ in range(num_items):
            product_id = random.randint(1, 20)
            quantity = random.randint(1, 3)
            unit_price = round(random.uniform(10, 200), 2)
            item_total = quantity * unit_price
            order_total += item_total
            
            order_items.append({
                'item_id': item_id,
                'order_id': order_num,
                'product_id': product_id,
                'quantity': quantity,
                'unit_price': unit_price
            })
            item_id += 1
        
        # Update order total
        orders[-1]['total_amount'] = round(order_total, 2)
    
    return pd.DataFrame(orders), pd.DataFrame(order_items)

def load_data_to_tables(customers_df, products_df, orders_df, order_items_df):
    """Load data to database tables in correct order"""
    try:
        # Load in correct order (parents first, then children)
        customers_df.to_sql('customers', engine, if_exists='append', index=False)
        print(" Customers loaded")
        
        products_df.to_sql('products', engine, if_exists='append', index=False)
        print(" Products loaded")
        
        orders_df.to_sql('orders', engine, if_exists='append', index=False)
        print(" Orders loaded")
        
        order_items_df.to_sql('order_items', engine, if_exists='append', index=False)
        print(" Order items loaded")
        
        return True
    except Exception as e:
        print(f" Error loading data: {e}")
        return False

def main():
    print(" Starting data generation...")
    
    # Step 1: Clear existing data
    if not clear_existing_data():
        return
    
    # Step 2: Create tables
    if not create_tables():
        return
    
    # Step 3: Generate data
    print(" Generating customers...")
    customers_df = generate_customers(50)
    
    print(" Generating products...")
    products_df = generate_products(20)
    
    print(" Generating orders and order items...")
    orders_df, order_items_df = generate_orders(100)
    
    # Step 4: Load to database
    print(" Loading data to MySQL...")
    if load_data_to_tables(customers_df, products_df, orders_df, order_items_df):
        print(" Data generation completed!")
        print(f" Generated: {len(customers_df)} customers, {len(products_df)} products, {len(orders_df)} orders, {len(order_items_df)} order items")
    else:
        print(" Data generation failed!")

if __name__ == "__main__":
    main()