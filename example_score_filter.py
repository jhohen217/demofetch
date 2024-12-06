import requests
import json

API_BASE_URL = "https://open.faceit.com/data/v4/matches/{match_id}/stats"
HEADERS = {
    "Authorization": "Bearer 258bb0ff-65c0-4145-ba7d-d9617986361b",
    "Accept": "application/json"
}

MATCH_ID = "1-6bfdc0bd-da96-49b7-b168-a6f2e9960b49"

def fetch_scoreboard(match_id):
    """
    Fetch the scoreboard JSON for a given match_id.
    """
    url = API_BASE_URL.format(match_id=match_id)
    response = requests.get(url, headers=HEADERS)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Failed to fetch data for match_id {match_id}: HTTP {response.status_code}")
        return None

def analyze_match(match_data):
    """
    Analyze the match data and return (has_ace, has_quad).
    """
    has_ace = False
    has_quad = False

    rounds = match_data.get("rounds", [])
    for rnd in rounds:
        teams = rnd.get("teams", [])
        for team in teams:
            players = team.get("players", [])
            for player in players:
                player_stats = player.get("player_stats", {})
                penta = int(player_stats.get("Penta Kills", "0"))
                quadro = int(player_stats.get("Quadro Kills", "0"))
                
                if penta > 0:
                    has_ace = True
                if quadro > 0:
                    has_quad = True

    return has_ace, has_quad

def main():
    match_data = fetch_scoreboard(MATCH_ID)
    if match_data is None:
        print("No match data available.")
        return

    has_ace, has_quad = analyze_match(match_data)

    # Decide category
    if has_ace and has_quad:
        category = "Ace+Quad (acequad)"
    elif has_ace:
        category = "Ace"
    elif has_quad:
        category = "Quad"
    else:
        category = "Unapproved (neither ace nor quad)"

    print(f"Match ID: {MATCH_ID}")
    print(f"Category: {category}")

if __name__ == "__main__":
    main()
