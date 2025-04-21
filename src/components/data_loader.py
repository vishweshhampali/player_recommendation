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

con.execute("DROP TABLE IF EXISTS players")
con.execute("""
CREATE TABLE players AS
SELECT DISTINCT player_name
FROM player_stats
""")

con.execute("DROP TABLE IF EXISTS player_overall_stats")
con.execute("""
CREATE TABLE player_overall_stats AS
SELECT
    player_name,
    COUNT(DISTINCT match_id) AS matches_played,
    SUM(runs_scored) AS total_runs,
    SUM(balls_faced) AS total_balls_faced,
    ROUND(SUM(runs_scored) * 100.0 / NULLIF(SUM(balls_faced), 0), 2) AS career_strike_rate,
    SUM(wickets) AS total_wickets,
    SUM(balls_bowled) AS total_balls_bowled,
    ROUND(SUM(runs_conceded) * 6.0 / NULLIF(SUM(balls_bowled), 0), 2) AS career_economy,
    COUNT(innings_batted) AS total_innings_batted,
    COUNT(innings_bowled) AS total_innings_bowled
FROM player_stats
GROUP BY player_name
""")

con.execute("DROP TABLE IF EXISTS match_overall_stats")
con.execute("""
CREATE TABLE match_overall_stats AS
SELECT
    match_id,
    match_format,
    SUM(runs_scored) AS total_runs_scored,
    SUM(wickets) AS total_wickets,
    SUM(balls_faced) AS total_balls,
    ROUND(SUM(runs_scored) * 100.0 / NULLIF(SUM(balls_faced), 0), 2) AS avg_strike_rate,
    SUM(balls_bowled) AS total_balls_bowled,
    ROUND(SUM(runs_conceded) * 6.0 / NULLIF(SUM(balls_bowled), 0), 2) AS avg_economy
FROM player_stats
GROUP BY match_id, match_format
""")

con.execute("DROP TABLE IF EXISTS match_team_stats")
con.execute("""
CREATE TABLE match_team_stats AS
SELECT
    match_id,
    team,
    SUM(runs_scored) AS team_runs,
    SUM(wickets) AS team_wickets,
    SUM(balls_faced) AS team_balls_faced,
    COUNT(DISTINCT player_name) AS players_played
FROM player_stats
GROUP BY match_id, team
""")

print(con.execute("SELECT COUNT(*) FROM players").fetchone())
print(con.execute("SELECT COUNT(*) FROM player_overall_stats").fetchone())
print(con.execute("SELECT COUNT(*) FROM match_overall_stats").fetchone())

con.close()
print("✅ DuckDB warehouse created at:", warehouse_path)
