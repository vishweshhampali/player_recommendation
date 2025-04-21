import json
import pandas as pd
from pathlib import Path
from collections import defaultdict
from tqdm import tqdm

BASE_DIR = Path(__file__).resolve().parent.parent.parent
print(BASE_DIR)
RAW_DATA_FOLDER = BASE_DIR / "data" / "raw" / "extracted"
print(RAW_DATA_FOLDER)
PROCESSED_FOLDER = BASE_DIR / "data" / "processed"
print(PROCESSED_FOLDER)
PROCESSED_FOLDER.mkdir(parents=True, exist_ok=True)

MATCH_FORMAT_FOLDERS = {
    "ODI": "odi",
    "T20I": "t20",
    "Test": "test"
}

def extract_player_stats_and_metadata(json_path, match_format):
    with open(json_path, 'r') as f:
        file_data = json.load(f)

    match_id = Path(json_path).stem
    info = file_data["info"]
    match_date = file_data["info"]["dates"][0]
    innings = file_data["innings"]

    player_stats = defaultdict(lambda: {
        "match_id": match_id,
        "match_date": match_date,
        "match_format": match_format,
        "team": None,
        "runs_scored": 0,
        "balls_faced": 0,
        "runs_conceded": 0,
        "balls_bowled": 0,
        "wickets": 0,
        "extras_conceded": 0,
        "innings_batted": None,
        "innings_bowled": None,
    })

    for inning_idx, inning in enumerate(innings, start=1):
        team = inning["team"]
        overs = inning["overs"]

        for over in overs:
            for delivery in over["deliveries"]:
                batter = delivery["batter"]
                bowler = delivery["bowler"]
                total_runs = delivery["runs"]["total"]
                batter_runs = delivery["runs"]["batter"]
                extras = delivery["runs"]["extras"]

                # Batting stats
                b_stats = player_stats[batter]
                b_stats["team"] = team
                b_stats["runs_scored"] += batter_runs

                if delivery["runs"]["extras"] == 0:
                    b_stats["balls_faced"] += 1
                b_stats["innings_batted"] = inning_idx

                # Bowling stats
                bo_stats = player_stats[bowler]
                bo_stats["team"] = [t for t in file_data["info"]["teams"] if t != team][0]
                bo_stats["runs_conceded"] += total_runs
                if delivery["runs"]["extras"] == 0:
                    bo_stats["balls_bowled"] += 1
                bo_stats["extras_conceded"] += extras
                bo_stats["innings_bowled"] = inning_idx

                # Wickets
                if "wickets" in delivery:
                    for w in delivery["wickets"]:
                        if w["kind"] != "run out":
                            bo_stats["wickets"] += 1

    player_data = []
    for player, stats in player_stats.items():
        stats["player_name"] = player
        player_data.append(stats)
    
    # Map player name to their register key if available
    player_registry = info.get("registry", {}).get("people", {})
    for stats in player_data:
        stats["player_code"] = player_registry.get(stats["player_name"], None)

    player_df = pd.DataFrame(player_data)
    
    match_metadata = {
        "match_id": match_id,
        "match_date": match_date,
        "match_format": match_format,
        "teams": " vs ".join(info.get("teams", [])),
        "team_type": info.get("team_type"),
        "event_name": info.get("event", {}).get("name", None),
        "event_match_no": info.get("event", {}).get("match_number", None),
        "venue": info.get("venue"),
        "city": info.get("city"),
        "stadium": info.get("venue"),  # alias
        "umpires": ", ".join(info.get("officials", {}).get("umpires", [])),
        "tv_umpires": ", ".join(info.get("officials", {}).get("tv_umpires", [])),
        "referee": ", ".join(info.get("officials", {}).get("match_referees", [])),
        "result": info.get("outcome", {}).get("result") or info.get("outcome", {}).get("winner", "no result"),
        "win_by": info.get("outcome", {}).get("by", None),
        "winner": info.get("outcome", {}).get("winner", None),
        "toss_winner": info.get("toss", {}).get("winner", None),
        "toss_decision": info.get("toss", {}).get("decision", None),
        "player_of_match": ", ".join(info.get("player_of_match", [])) if "player_of_match" in info else None,
        "overs": info.get("overs"),
        "gender": info.get("gender"),
        "match_type": info.get("match_type"),
    }

    return player_df, match_metadata

def transform_all_matches_to_parquet():
    player_stats_df = pd.DataFrame()
    match_info_list = []
    
    for match_format, folder in MATCH_FORMAT_FOLDERS.items():
        match_dir = RAW_DATA_FOLDER / folder
        if not match_dir.exists():
            print(f"Folder not found for {match_format}: {match_dir}")
            continue

        json_files = list(match_dir.glob("*.json"))
        print(f"Processing {len(json_files)} {match_format} matches...")

        for file in tqdm(json_files, desc=f"Processing {match_format}"):
            try:
                player_df, match_meta = extract_player_stats_and_metadata(file, match_format)
                player_stats_df = pd.concat([player_stats_df, player_df], ignore_index=True)
                match_info_list.append(match_meta)
            except Exception as e:
                print(f"Failed to process {file.name}: {e}")

    # Save player stats
    output_parquet = PROCESSED_FOLDER / "player_stats.parquet"
    output_csv = PROCESSED_FOLDER / "player_stats.csv"
    player_stats_df.to_parquet(output_parquet, index=False)
    player_stats_df.to_csv(output_csv, index=False)
    print(f"\n✅ Player stats saved to:\n  - Parquet: {output_parquet}\n  - CSV: {output_csv}")

    # Save match info
    match_df = pd.DataFrame(match_info_list)
    output_meta_parquet = PROCESSED_FOLDER / "match_info.parquet"
    output_meta_csv = PROCESSED_FOLDER / "match_info.csv"
    match_df.to_parquet(output_meta_parquet, index=False)
    match_df.to_csv(output_meta_csv, index=False)
    print(f"\n✅ Match metadata saved to:\n  - Parquet: {output_meta_parquet}\n  - CSV: {output_meta_csv}")


if __name__ == "__main__":
    transform_all_matches_to_parquet()

