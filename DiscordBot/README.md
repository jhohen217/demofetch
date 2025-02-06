# DemoFetch

A Discord bot for downloading, analyzing, and managing CS2 demo files from FACEIT matches.

## Features

- Fetch and download FACEIT match demos
- Categorize demos (ace, quad)
- Parse demos for kill sequences
- Efficient storage management
- Detailed match analysis

## Project Structure

```
demofetch/
├── commands/      # Command handlers
├── core/          # Core functionality
├── demos/         # Downloaded demo files
├── parsed/        # Parsed demo results
│   ├── ace/       # Matches with ace kills
│   └── quad/      # Matches with quad kills
├── usermatches/   # User match ID files
└── config.json
```

## Commands

### Management Commands
- `/fetch <username>`: Fetch FACEIT matches
- `/download <target> [number]`: Download demos
  - Target: username or category (auto/ace/quad)
- `/info`: View storage and match statistics
- `/start`: Begin match ID fetching
- `/stop [service]`: Stop services
  - No param: Stop all services
  - `fetch`: Stop match fetching
  - `download`: Stop demo downloading
  - `parse`: Stop demo parsing

### Parsing Commands
- `/parse <category> [number]`
  - `category`: ace, quad
  - `number`: Optional limit of demos to parse
  - Reads from category_matchids.txt
  - Saves results in parsed/category/

## Demo Parsing Output

```json
{
  "demoInfo": {
    "fileName": "example_match.dem",
    "mapName": "de_map",
    "date": "2024-01-01 12:00:00",
    "aceKills": 1,
    "quadKills": 2,
    "multiKills": 1
  },
  "aceKills": [
    {
      "roundNumber": 5,
      "player": "Player1",
      "killCount": 5,
      "duration_in_ticks": 128,
      "kills": [
        {
          "attacker": {
            "name": "Player1",
            "steam64id": "STEAM_X:X:XXXXXXXX",
            "team": "CT",
            "position": {"x": 0.0, "y": 0.0, "z": 0.0},
            "viewAngles": {"pitch": 0.0, "yaw": 0.0}
          },
          "victim": {
            "name": "Player2",
            "steam64id": "STEAM_X:X:XXXXXXXX",
            "team": "T",
            "position": {"x": 0.0, "y": 0.0, "z": 0.0}
          },
          "killDetails": {
            "tick": 1234,
            "weapon": "weapon_ak47",
            "penetrationKill": false,
            "headshot": true,
            "noscope": false,
            "thrusmoke": false,
            "distance_to_enemy": 500.0,
            "ticks_since_last_kill": 0,
            "distance_moved_since_last_kill": 0.0
          }
        }
      ],
      "metadata": {
        "weapons_used": ["weapon_ak47"],
        "headshots": 1,
        "wallbangs": 0,
        "radius_moved": 100.0,
        "victims_in_radius": 200.0,
        "total_distance_moved": 300.0
      }
    }
  ]
}
```

## Setup

1. Copy `config.json.example` to `config.json`
2. Install dependencies: `pip install -r requirements.txt`
3. Run: `python DiscordBotStart.py`

## Dependencies

- nextcord
- demoparser2
- pandas
- requests
- aiohttp
- urllib3

## Notes

- Owner permissions required
- Efficient demo storage and parsing
- Preserves original .dem files if no .dem.gz exists
- Uses prefixed match IDs (e.g., "0100_matchid" for 1 ace, 0 quads)
