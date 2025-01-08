import os
import math
from datetime import datetime
from collections import defaultdict
from typing import Dict, List, Optional, Tuple
from .demo_parser_utils import (
    calculate_distance, calculate_radius, calculate_fight_radius,
    calculate_total_distance, is_valid_kill, group_weapons_by_type,
    sanitize_nan_values
)

def parse_demo(dem_file_path):
    """
    Comprehensive demo parsing with extensive error handling and logging
    Returns: Tuple[Optional[Dict], Optional[DemoParser]] - The parsed data and parser instance
    """
    try:
        # Import required libraries
        try:
            from demoparser2 import DemoParser
            import pandas as pd
        except ImportError as import_error:
            print(f"CRITICAL IMPORT ERROR: {import_error}")
            print("Ensure demoparser2 and pandas are installed:")
            print("pip install demoparser2 pandas")
            raise

        # Initialize DemoParser
        parser = DemoParser(dem_file_path)
        header = parser.parse_header()
        
        # Parse events
        df = parser.parse_event("player_death", 
                               player=["X", "Y", "Z", "pitch", "yaw", "team_name"], 
                               other=["total_rounds_played"])

        # Parsing logic with extensive error tracking
        kills_by_round = defaultdict(lambda: defaultdict(list))
        kills_by_tick = defaultdict(list)
        all_kills = []
        
        for idx, row in df.iterrows():
            try:
                round_num = int(row["total_rounds_played"])
                tick = int(row.get("tick", 0))
                
                # Skip round 0 (warmup)
                if round_num == 0:
                    continue
                
                # Skip invalid weapons (world kills, bomb kills, etc)
                weapon = str(row.get("weapon", "")).lower()
                if weapon in {"world", "planted_c4"}:
                    continue
                
                # Skip kills with invalid names
                if (str(row.get("attacker_name", "")) in ["None", "Unknown"] or 
                    str(row.get("user_name", "")) in ["None", "Unknown"]):
                    continue
                
                # Skip kills with NaN positions
                try:
                    attacker_pos = {
                        'x': float(row.get('attacker_X')),
                        'y': float(row.get('attacker_Y')),
                        'z': float(row.get('attacker_Z'))
                    }
                    victim_pos = {
                        'x': float(row.get('user_X')),
                        'y': float(row.get('user_Y')),
                        'z': float(row.get('user_Z'))
                    }
                    if any(math.isnan(coord) for coord in attacker_pos.values()) or \
                       any(math.isnan(coord) for coord in victim_pos.values()):
                        continue
                except (ValueError, TypeError):
                    continue
                
                # Comprehensive kill info extraction with error handling
                kill_info = {
                    "attacker": {
                        "name": str(row.get("attacker_name", "Unknown")),
                        "steam64id": str(row.get("attacker_steamid", "Unknown")),
                        "team": str(row.get("attacker_team_name", "Unknown")),
                        "position": attacker_pos,
                        "viewAngles": {
                            "pitch": float(row.get('attacker_pitch')),
                            "yaw": float(row.get('attacker_yaw'))
                        }
                    },
                    "victim": {
                        "name": str(row.get("user_name", "Unknown")),
                        "steam64id": str(row.get("user_steamid", "Unknown")),
                        "team": str(row.get("user_team_name", "Unknown")),
                        "position": victim_pos
                    },
                    "killDetails": {
                        "tick": tick,
                        "weapon": str(row.get("weapon", "Unknown")),
                        "penetrationKill": bool(row.get("penetrated", False)),
                        "headshot": bool(row.get("headshot", False)),
                        "noscope": bool(row.get("noscope", False)),
                        "thrusmoke": bool(row.get("thrusmoke", False)),
                        "distance_to_enemy": float(row.get("distance", 0))
                    }
                }

                # Assist info with robust error handling
                if row.get("assister_name"):
                    kill_info["assist"] = {
                        "name": str(row.get("assister_name", "Unknown")),
                        "steam64id": str(row.get("assister_steamid", "Unknown")),
                        "team": str(row.get("assister_team_name", "Unknown")),
                        "flashAssist": bool(row.get("assistedflash", False))
                    }

                if is_valid_kill(kill_info):
                    all_kills.append(kill_info)
                    attacker = str(row.get("attacker_name", "Unknown"))
                    kills_by_round[round_num][attacker].append(kill_info)
                    kills_by_tick[f"{round_num}_{tick}"].append(kill_info)
            
            except Exception as row_process_error:
                print(f"Error processing row {idx}: {row_process_error}")
                continue

        # Process kills into separate collections
        ace_kills = []
        quad_kills = []
        triple_kills = []
        multi_kills = []
        
        # Process ace, quad, and triple kills by round
        for round_num, players in kills_by_round.items():
            for player, kills in players.items():
                # Sort kills by tick for consistent ordering
                kills.sort(key=lambda x: x["killDetails"]["tick"])
                
                # Create sequence info for all kills in the round
                if len(kills) >= 3:  # Process if player got 3 or more kills
                    # Calculate ticks since last kill and distance moved
                    last_tick = kills[0]["killDetails"]["tick"]
                    last_pos = kills[0]["attacker"]["position"]
                    for i, kill in enumerate(kills):
                        current_tick = kill["killDetails"]["tick"]
                        current_pos = kill["attacker"]["position"]
                        
                        kill["killDetails"]["ticks_since_last_kill"] = current_tick - last_tick if i > 0 else 0
                        kill["killDetails"]["distance_moved_since_last_kill"] = (
                            calculate_distance(last_pos, current_pos) if i > 0 else 0.0
                        )
                        
                        last_tick = current_tick
                        last_pos = current_pos

                    # Calculate radii and total distance
                    attacker_positions = [k["attacker"]["position"] for k in kills]
                    victim_positions = [k["victim"]["position"] for k in kills]

                    sequence_info = {
                        "roundNumber": round_num,
                        "player": player,
                        "killCount": len(kills),
                        "duration_in_ticks": kills[-1]["killDetails"]["tick"] - kills[0]["killDetails"]["tick"],
                        "kills": kills,
                        "metadata": {
                            "weapons_used": list(set(k["killDetails"]["weapon"] for k in kills)),
                            "headshots": sum(1 for k in kills if k["killDetails"]["headshot"]),
                            "wallbangs": sum(1 for k in kills if k["killDetails"]["penetrationKill"]),
                            "radius_moved": calculate_radius(attacker_positions),
                            "victims_in_radius": calculate_radius(victim_positions),
                            "total_distance_moved": calculate_total_distance(attacker_positions)
                        }
                    }
                    
                    if len(kills) == 5:  # Ace
                        ace_kills.append(sequence_info)
                    elif len(kills) == 4:  # Quad
                        quad_kills.append(sequence_info)
                    elif len(kills) == 3:  # Triple
                        triple_kills.append(sequence_info)

        # Process multi-kills (3+ kills on same tick)
        for tick_key, kills in kills_by_tick.items():
            round_num = int(tick_key.split('_')[0])
            if len(kills) >= 3:  # Only include 3+ kills on same tick
                kills_by_player = defaultdict(list)
                for kill in kills:
                    attacker = kill["attacker"]["name"]
                    kills_by_player[attacker].append(kill)
                
                for player, player_kills in kills_by_player.items():
                    if len(player_kills) >= 3:  # Only include if single player got 3+ kills
                        # For multi-kills, calculate fight radius (includes attacker and all victims)
                        attacker_pos = player_kills[0]["attacker"]["position"]  # Same position for all kills
                        victim_positions = [k["victim"]["position"] for k in player_kills]

                        # Simplified kill info for multi-kills (remove timing/movement fields)
                        simplified_kills = []
                        for kill in player_kills:
                            kill_copy = kill.copy()
                            kill_copy["killDetails"] = {
                                "tick": kill["killDetails"]["tick"],
                                "weapon": kill["killDetails"]["weapon"],
                                "penetrationKill": kill["killDetails"]["penetrationKill"],
                                "headshot": kill["killDetails"]["headshot"],
                                "noscope": kill["killDetails"]["noscope"],
                                "thrusmoke": kill["killDetails"]["thrusmoke"],
                                "distance_to_enemy": kill["killDetails"]["distance_to_enemy"]
                            }
                            simplified_kills.append(kill_copy)

                        multi_kills.append({
                            "roundNumber": round_num,
                            "player": player,
                            "killCount": len(player_kills),
                            "duration_in_ticks": 0,
                            "kills": simplified_kills,
                            "metadata": {
                                "weapons_used": list(set(k["killDetails"]["weapon"] for k in player_kills)),
                                "headshots": sum(1 for k in player_kills if k["killDetails"]["headshot"]),
                                "wallbangs": sum(1 for k in player_kills if k["killDetails"]["penetrationKill"]),
                                "fight_radius": calculate_fight_radius(attacker_pos, victim_positions)
                            }
                        })

        # Sort collections by round number
        ace_kills.sort(key=lambda x: x["roundNumber"])
        quad_kills.sort(key=lambda x: x["roundNumber"])
        triple_kills.sort(key=lambda x: x["roundNumber"])
        multi_kills.sort(key=lambda x: x["roundNumber"])

        # Get map name from header and format date from file creation time
        map_name = header.get('map_name', 'unknown_map') if header else 'unknown_map'
        creation_time = os.path.getctime(dem_file_path)
        date = datetime.fromtimestamp(creation_time).strftime('%Y-%m-%d %H:%M:%S')

        # Group weapons by kill type
        ace_weapons = group_weapons_by_type(ace_kills)
        quad_weapons = group_weapons_by_type(quad_kills)
        triple_weapons = group_weapons_by_type(triple_kills)
        multi_weapons = group_weapons_by_type(multi_kills)

        # Format the output string
        filename = os.path.basename(dem_file_path).replace('.dem', '')
        output_str = (
            f"{filename}: "
            f"{len(ace_kills)} Ace kills {' '.join(ace_weapons) if ace_weapons else '[]'}, "
            f"{len(quad_kills)} Quad kills {' '.join(quad_weapons) if quad_weapons else '[]'}, "
            f"{len(triple_kills)} Triple kills {' '.join(triple_weapons) if triple_weapons else '[]'}, "
            f"{len(multi_kills)} multi-kills {' '.join(multi_weapons) if multi_weapons else '[]'}"
        )
        print(output_str)

        output_json = {
            "demoInfo": {
                "fileName": os.path.basename(dem_file_path),
                "mapName": map_name,
                "date": date,
                "aceKills": len(ace_kills),
                "quadKills": len(quad_kills),
                "tripleKills": len(triple_kills),
                "multiKills": len(multi_kills)
            },
            "aceKills": ace_kills,
            "quadKills": quad_kills,
            "tripleKills": triple_kills,
            "multiKills": multi_kills
        }

        # Only return the JSON and parser if there are actual kills
        if len(ace_kills) > 0 or len(quad_kills) > 0 or len(triple_kills) > 0 or len(multi_kills) > 0:
            return sanitize_nan_values(output_json), parser
        return None, None

    except Exception as critical_error:
        print(f"CRITICAL PARSING ERROR: {critical_error}")
        print(traceback.format_exc())
        raise
