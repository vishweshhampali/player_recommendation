import duckdb
import pandas as pd
from pathlib import Path
import os
import numpy as np

BASE_DIR = Path(__file__).resolve().parent.parent.parent
# File path for your warehouse
warehouse_path = BASE_DIR/"data/warehouse/cricket_warehouse.duckdb"

# Create the folder if not exists

os.makedirs(os.path.dirname(warehouse_path), exist_ok=True)

# Connect to DuckDB
con = duckdb.connect(warehouse_path)

player_df = pd.read_parquet(BASE_DIR/"data/processed/player_stats.parquet")
match_df = pd.read_parquet(BASE_DIR/"data/processed/match_info.parquet")


player_df["strike_rate"] = np.where(
    player_df["balls_faced"] > 0,
    (player_df["runs_scored"] / player_df["balls_faced"]) * 100,
    0
)
player_df["economy"] = np.where(
    player_df["balls_bowled"] > 0,
    (player_df["runs_conceded"] / player_df["balls_bowled"]) * 6,
    0
)

con.execute("DROP TABLE IF EXISTS match_info")
con.execute("DROP TABLE IF EXISTS player_stats")

con.register("match_df", match_df)
con.register("player_df", player_df)

# Create match_info table
con.execute("""
CREATE TABLE match_info AS
SELECT * FROM match_df
""")

# Create player_stats table
con.execute("""
CREATE TABLE player_stats AS
SELECT * FROM player_df
""")

print(con.execute("SELECT COUNT(*) FROM match_info").fetchall())
print(con.execute("SELECT COUNT(*) FROM player_stats").fetchall())

con.close()
print("✅ DuckDB warehouse created at:", warehouse_path)
