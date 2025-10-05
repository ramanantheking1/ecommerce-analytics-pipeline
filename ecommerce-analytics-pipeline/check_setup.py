# check_setup.py
import sys
print(f"Python version: {sys.version}")

# Check essential libraries
try:
    import pandas as pd
    print("✅ Pandas installed")
except ImportError:
    print("❌ Pandas missing")

try:
    import pymysql
    print("✅ PyMySQL installed")
except ImportError:
    print("❌ PyMySQL missing")

try:
    import sqlalchemy
    print("✅ SQLAlchemy installed")
except ImportError:
    print("❌ SQLAlchemy missing")

print("Setup check completed!")