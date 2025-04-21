import duckdb
import pandas as pd
from pathlib import Path
import os
import numpy as np

BASE_DIR = Path(__file__).resolve().parent.parent.parent
# File path for your warehouse
warehouse_path = BASE_DIR/"data/warehouse/cricket_warehouse.duckdb"

# Connect to DuckDB
con = duckdb.connect(warehouse_path)

print(con.execute("SELECT * FROM match_overall_stats").fetchone())

con.close()
print("✅ DuckDB warehouse created at:", warehouse_path)