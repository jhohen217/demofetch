import os
import math
import json
import traceback
from datetime import datetime
from collections import defaultdict
from typing import Dict, List, Optional, Tuple

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

def group_weapons_by_type(kills_list, metadata_key="metadata"):
    """Helper function to group weapons for output formatting"""
    weapons = []
    for item in kills_list:
        item_weapons = item[metadata_key]["weapons_used"]
        if len(item_weapons) > 2:
            # Split weapons into groups of 2
            for i in range(0, len(item_weapons), 2):
                group = item_weapons[i:i+2]
                if group:  # Only add non-empty groups
                    weapons.append(f"[{', '.join(group)}]")
        else:
            weapons.append(f"[{', '.join(item_weapons)}]" if item_weapons else "[]")
    return weapons
