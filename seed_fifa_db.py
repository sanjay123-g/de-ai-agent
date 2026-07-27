"""
seed_fifa_db.py
Seeds fifa.db with 100% REAL data from openfootball's public domain API:
  - national_teams (48 rows): real WC 2026 teams + groups
  - player_profiles (1248 rows): real full squads — name, position,
    jersey number, club, date of birth
Source: https://raw.githubusercontent.com/openfootball/worldcup.json/master/2026/worldcup.squads.json
"""
import sqlite3
import httpx
from pathlib import Path

DB_PATH = Path("data/fifa.db")
SQUADS_URL = "https://raw.githubusercontent.com/openfootball/worldcup.json/master/2026/worldcup.squads.json"

def seed():
    if DB_PATH.exists():
        print(f"{DB_PATH} already exists. Delete it first to re-seed.")
        return

    resp = httpx.get(SQUADS_URL, timeout=30, follow_redirects=True)
    resp.raise_for_status()
    squads = resp.json()
    print(f"Fetched {len(squads)} real teams from openfootball squads API")

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE national_teams (
            team_name  TEXT PRIMARY KEY,
            fifa_code  TEXT,
            group_name TEXT NOT NULL
        )
    """)

    cur.execute("""
        CREATE TABLE player_profiles (
            player_id     TEXT PRIMARY KEY,
            player_name   TEXT NOT NULL,
            team_name     TEXT NOT NULL,
            position      TEXT NOT NULL,
            jersey_number INTEGER,
            club_name     TEXT,
            club_country  TEXT,
            date_of_birth TEXT
        )
    """)

    team_rows = []
    player_rows = []
    pid = 1
    for team in squads:
        team_name = team.get("name", "")
        team_rows.append((team_name, team.get("fifa_code", ""), team.get("group", "")))
        for p in team.get("players", []):
            club = p.get("club") or {}
            player_rows.append((
                f"P{pid:05d}",
                p.get("name", ""),
                team_name,
                p.get("pos", ""),
                p.get("number"),
                club.get("name", ""),
                club.get("country", ""),
                p.get("date_of_birth", ""),
            ))
            pid += 1

    cur.executemany("INSERT INTO national_teams VALUES (?, ?, ?)", team_rows)
    cur.executemany("INSERT INTO player_profiles VALUES (?, ?, ?, ?, ?, ?, ?, ?)", player_rows)

    conn.commit()
    nt = cur.execute("SELECT COUNT(*) FROM national_teams").fetchone()[0]
    pp = cur.execute("SELECT COUNT(*) FROM player_profiles").fetchone()[0]
    print(f"Seeded national_teams: {nt} rows (real)")
    print(f"Seeded player_profiles: {pp} rows (real)")
    conn.close()

if __name__ == "__main__":
    seed()
