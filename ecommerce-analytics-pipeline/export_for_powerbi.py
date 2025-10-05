# export_for_powerbi.py - EXPORT DATA FOR POWER BI
import pandas as pd
from sqlalchemy import create_engine

# Database connection
dw_engine = create_engine('mysql+pymysql://root:@localhost/ecommerce_dw')

def export_data_for_powerbi():
    print(" Exporting data for Power BI...")
    
    try:
        # Export dim_customer
        dim_customer = pd.read_sql("SELECT * FROM dim_customer", dw_engine)
        dim_customer.to_csv('dim_customer.csv', index=False)
        print(" Exported dim_customer.csv")
        
        # Export dim_product
        dim_product = pd.read_sql("SELECT * FROM dim_product", dw_engine)
        dim_product.to_csv('dim_product.csv', index=False)
        print(" Exported dim_product.csv")
        
        # Export dim_date
        dim_date = pd.read_sql("SELECT * FROM dim_date", dw_engine)
        dim_date.to_csv('dim_date.csv', index=False)
        print(" Exported dim_date.csv")
        
        # Export fact_sales
        fact_sales = pd.read_sql("SELECT * FROM fact_sales", dw_engine)
        fact_sales.to_csv('fact_sales.csv', index=False)
        print(" Exported fact_sales.csv")
        
        print("\n ALL FILES EXPORTED!")
        print(" Files created in your current directory:")
        print("   - dim_customer.csv")
        print("   - dim_product.csv") 
        print("   - dim_date.csv")
        print("   - fact_sales.csv")
        print("\n Now import these CSV files into Power BI!")
        
    except Exception as e:
        print(f" Export failed: {e}")

if __name__ == "__main__":
    choice = input("Export CSV files for Power BI? (y/n): ")
    if choice.lower() == 'y':
        export_data_for_powerbi()
    else:
        print(" Power BI can connect directly to MySQL database!")