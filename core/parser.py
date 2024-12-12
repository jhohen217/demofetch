import os
import sys
import json
import gzip
import shutil
import traceback
import platform
import importlib
import asyncio
import math
from collections import defaultdict
from datetime import datetime
from typing import Dict, List, Optional, Set, Tuple

class NaNEncoder(json.JSONEncoder):
    """Custom JSON encoder that converts NaN values to null"""
    def default(self, obj):
        try:
            if isinstance(obj, float) and math.isnan(obj):
                return None
        except:
            pass
        return super().default(obj)

def sanitize_nan_values(obj):
    """Recursively convert NaN values to None in a nested structure"""
    if isinstance(obj, dict):
        return {k: sanitize_nan_values(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [sanitize_nan_values(x) for x in obj]
    elif isinstance(obj, float) and math.isnan(obj):
        return None
    return obj

class DemoParser:
    def __init__(self):
        self.stop_flag = False

    def reset_stop_flag(self):
        """Reset the stop flag to allow parsing to continue"""
        self.stop_flag = False

    def stop_parsing(self):
        """Set the stop flag to stop parsing"""
        self.stop_flag = True

    async def parse_demo(self, dem_file_path: str, match_id: str, category: str) -> Optional[Dict]:
        """Parse a demo file and return the results"""
        if self.stop_flag:
            return None
        
        try:
            output_json = parse_demo(dem_file_path)
            return output_json
        except Exception as e:
            print(f"Error parsing demo {match_id}: {str(e)}")
            return None

    async def add_to_queue(self, match_ids: List[str]) -> int:
        """Add matches to the parsing queue"""
        # Setup paths
        base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        textfiles_path = os.path.join(base_path, "textfiles")
        parsed_txt_path = os.path.join(textfiles_path, "parsed.txt")
        queue_path = os.path.join(textfiles_path, "parse_queue.txt")

        return add_to_queue(queue_path, match_ids, parsed_txt_path)

    async def get_next_match(self) -> Optional[str]:
        """Get the next match from the queue"""
        if self.stop_flag:
            return None

        base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        queue_path = os.path.join(base_path, "textfiles", "parse_queue.txt")
        
        current_queue = read_queue(queue_path)
        if not current_queue:
            return None
            
        return current_queue[0]

def calculate_distance(pos1: Dict[str, float], pos2: Dict[str, float]) -> float:
    """Calculate 3D distance between two positions"""
    try:
        return math.sqrt(
            (pos2['x'] - pos1['x'])**2 +
            (pos2['y'] - pos1['y'])**2 +
            (pos2['z'] - pos1['z'])**2
        )
    except (TypeError, ValueError):
        return 0.0

def calculate_radius(positions: List[Dict[str, float]]) -> float:
    """Calculate minimum radius that encompasses all positions"""
    if not positions:
        return 0.0
    
    try:
        # Find center point (average of all positions)
        center = {
            'x': sum(p['x'] for p in positions) / len(positions),
            'y': sum(p['y'] for p in positions) / len(positions),
            'z': sum(p['z'] for p in positions) / len(positions)
        }
        
        # Find maximum distance from center to any point
        return max(calculate_distance(center, pos) for pos in positions)
    except (TypeError, ValueError):
        return 0.0

def calculate_fight_radius(attacker_pos: Dict[str, float], victim_positions: List[Dict[str, float]]) -> float:
    """Calculate minimum radius that encompasses attacker and all victims"""
    all_positions = [attacker_pos] + victim_positions
    return calculate_radius(all_positions)

def calculate_total_distance(positions: List[Dict[str, float]]) -> float:
    """Calculate total distance traveled between positions"""
    if len(positions) < 2:
        return 0.0
    
    try:
        total = 0.0
        for i in range(1, len(positions)):
            total += calculate_distance(positions[i-1], positions[i])
        return total
    except (TypeError, ValueError):
        return 0.0

def is_valid_kill(kill_info: Dict) -> bool:
    """Check if a kill is valid (not a disconnect or invalid data)"""
    # Check for invalid weapons (disconnects, bomb, etc)
    invalid_weapons = {"world", "planted_c4"}
    if kill_info["killDetails"]["weapon"].lower() in invalid_weapons:
        return False
    
    # Check for None or invalid names
    if (kill_info["attacker"]["name"] in ["None", "Unknown"] or 
        kill_info["victim"]["name"] in ["None", "Unknown"]):
        return False
    
    # Check for NaN positions
    attacker_pos = kill_info["attacker"]["position"]
    victim_pos = kill_info["victim"]["position"]
    
    try:
        for pos in [attacker_pos, victim_pos]:
            for coord in pos.values():
                if math.isnan(float(coord)):
                    return False
    except (ValueError, TypeError):
        return False
    
    return True

def parse_demo(dem_file_path):
    """
    Comprehensive demo parsing with extensive error handling and logging
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
        ace_weapons = []
        for ace in ace_kills:
            weapons = ace["metadata"]["weapons_used"]
            if len(weapons) > 2:
                # Split weapons into groups of 2
                for i in range(0, len(weapons), 2):
                    group = weapons[i:i+2]
                    if group:  # Only add non-empty groups
                        ace_weapons.append(f"[{', '.join(group)}]")
            else:
                ace_weapons.append(f"[{', '.join(weapons)}]")

        quad_weapons = []
        for quad in quad_kills:
            weapons = quad["metadata"]["weapons_used"]
            if len(weapons) > 2:
                # Split weapons into groups of 2
                for i in range(0, len(weapons), 2):
                    group = weapons[i:i+2]
                    if group:  # Only add non-empty groups
                        quad_weapons.append(f"[{', '.join(group)}]")
            else:
                quad_weapons.append(f"[{', '.join(weapons)}]")

        triple_weapons = []
        for triple in triple_kills:
            weapons = triple["metadata"]["weapons_used"]
            if len(weapons) > 2:
                # Split weapons into groups of 2
                for i in range(0, len(weapons), 2):
                    group = weapons[i:i+2]
                    if group:  # Only add non-empty groups
                        triple_weapons.append(f"[{', '.join(group)}]")
            else:
                triple_weapons.append(f"[{', '.join(weapons)}]")

        multi_weapons = []
        for multi in multi_kills:
            weapons = multi["metadata"]["weapons_used"]
            if len(weapons) > 2:
                # Split weapons into groups of 2
                for i in range(0, len(weapons), 2):
                    group = weapons[i:i+2]
                    if group:  # Only add non-empty groups
                        multi_weapons.append(f"[{', '.join(group)}]")
            else:
                multi_weapons.append(f"[{', '.join(weapons)}]" if weapons else "[]")

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

        # Only return the JSON if there are actual kills
        if len(ace_kills) > 0 or len(quad_kills) > 0 or len(triple_kills) > 0 or len(multi_kills) > 0:
            return sanitize_nan_values(output_json)
        return None

    except Exception as critical_error:
        print(f"CRITICAL PARSING ERROR: {critical_error}")
        print(traceback.format_exc())
        raise

def mark_as_parsed(parsed_txt_path: str, match_id: str):
    """Mark a match as parsed, avoiding duplicates"""
    # Read existing parsed matches
    parsed_matches = set()
    if os.path.exists(parsed_txt_path):
        with open(parsed_txt_path, 'r', encoding='utf-8') as f:
            parsed_matches = {line.strip() for line in f if line.strip()}
    
    # Only append if not already parsed
    if match_id not in parsed_matches:
        with open(parsed_txt_path, 'a', encoding='utf-8') as f:
            f.write(match_id + "\n")

def read_queue(queue_path: str) -> List[str]:
    """Read match IDs from queue"""
    if not os.path.exists(queue_path):
        return []
    with open(queue_path, 'r', encoding='utf-8') as f:
        return [line.strip() for line in f if line.strip()]

def write_queue(queue_path: str, match_ids: List[str]):
    """Write match IDs to queue"""
    with open(queue_path, 'w', encoding='utf-8') as f:
        for match_id in match_ids:
            f.write(f"{match_id}\n")

def add_to_queue(queue_path: str, match_ids: List[str], parsed_txt_path: str) -> int:
    """Add matches to the parsing queue if not already parsed"""
    # Read existing parsed matches and current queue
    parsed_matches = set()
    if os.path.exists(parsed_txt_path):
        with open(parsed_txt_path, 'r', encoding='utf-8') as f:
            parsed_matches = {line.strip() for line in f if line.strip()}
    
    current_queue = read_queue(queue_path)
    current_queue_set = set(current_queue)
    
    added_count = 0
    for match_id in match_ids:
        if match_id not in parsed_matches and match_id not in current_queue_set:
            current_queue.append(match_id)
            added_count += 1
    
    if added_count > 0:
        write_queue(queue_path, current_queue)
    
    print(f"Added {added_count} matches to queue")
    print("--- EVENT PARSING ---")
    return added_count

async def process_matches(match_ids: List[str], base_path: str, category: str):
    """Process multiple matches with queue management"""
    # Setup paths
    demos_path = os.path.join(base_path, "public_demos")
    parsed_path = os.path.join(base_path, "parsed", category)
    textfiles_path = os.path.join(base_path, "textfiles")

    os.makedirs(parsed_path, exist_ok=True)
    os.makedirs(textfiles_path, exist_ok=True)

    parsed_txt_path = os.path.join(textfiles_path, "parsed.txt")
    queue_path = os.path.join(textfiles_path, "parse_queue.txt")

    # Add matches to queue
    added = add_to_queue(queue_path, match_ids, parsed_txt_path)
    if added == 0:
        print("No new matches to process")
        return

    # Process queue
    current_queue = read_queue(queue_path)
    for match_id in current_queue:
        # Get base match ID without prefix for finding the demo file
        base_match_id = match_id.split('-', 1)[1] if '-' in match_id else match_id
        
        dem_filename = f"{base_match_id}.dem"
        dem_gz_filename = f"{base_match_id}.dem.gz"

        dem_file_path = os.path.join(demos_path, dem_filename)
        dem_gz_file_path = os.path.join(demos_path, dem_gz_filename)

        # File existence and extraction logic
        used_gz = False
        if not os.path.isfile(dem_file_path):
            if os.path.isfile(dem_gz_file_path):
                with gzip.open(dem_gz_file_path, 'rb') as f_in:
                    with open(dem_file_path, 'wb') as f_out:
                        shutil.copyfileobj(f_in, f_out)
                used_gz = True
            else:
                print(f"No demo found for match_id {match_id}.")
                continue

        try:
            parser = DemoParser()
            output_json = await parser.parse_demo(dem_file_path, match_id, category)
            if output_json:
                # Save parsed data with prefixed match ID
                output_json_path = os.path.join(parsed_path, f"{match_id}.json")
                with open(output_json_path, 'w', encoding='utf-8') as f:
                    json.dump(output_json, f, indent=4, cls=NaNEncoder)
                
                # Mark as parsed with prefixed match ID
                mark_as_parsed(parsed_txt_path, match_id)

            # Cleanup extracted file
            if used_gz and os.path.isfile(dem_file_path):
                os.remove(dem_file_path)

        except Exception as e:
            print(f"Error processing match {match_id}: {e}")
            print(traceback.format_exc())
            continue

        # Update queue after successful processing
        remaining_queue = [m for m in read_queue(queue_path) if m != match_id]
        write_queue(queue_path, remaining_queue)
