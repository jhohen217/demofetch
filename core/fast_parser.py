import os
import subprocess
import json
from typing import Dict, List, Optional

class FastDemoParserWrapper:
    """Wrapper for the C# BlenderParser to match the Python parser interface"""
    
    def __init__(self, demo_parser, map_name: str, demo_file: str):
        self.parser = demo_parser  # Original C# parser instance
        self.map_name = map_name
        self.demo_file = demo_file
        self._cached_data = None
        
    def _ensure_data_loaded(self):
        """Ensure the demo data is loaded and cached"""
        if self._cached_data is None:
            # Get the path to the C# parser executable
            parser_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 
                                    "CSharpParser", "demofile-net", "examples",
                                    "DemoFile.Example.FastParser")
            
            # Run the C# parser and get the output
            print("Running C# parser...")
            print(f"Parser directory: {parser_dir}")
            print(f"Demo file: {self.demo_file}")
            output_path = os.path.join(parser_dir, "temp_output.json")
            print(f"Output path: {output_path}")
            
            exe_path = os.path.join(parser_dir, "bin", "Debug", "net7.0", "DemoFile.Example.FastParser.dll")
            result = subprocess.run([
                "dotnet", exe_path,
                "--demo", self.demo_file,
                "--output", output_path
            ], capture_output=True, text=True)
            
            print("C# parser stdout:", result.stdout)
            print("C# parser stderr:", result.stderr)
            
            if result.returncode != 0:
                raise Exception(f"C# parser failed: {result.stderr}")
            
            # Load the generated JSON file
            try:
                print(f"Attempting to read output file: {output_path}")
                if os.path.exists(output_path):
                    print(f"Output file exists, size: {os.path.getsize(output_path)} bytes")
                    with open(output_path, "r") as f:
                        content = f.read()
                        print(f"File content length: {len(content)} chars")
                        self._cached_data = json.loads(content)
                        print("Successfully loaded JSON data")
                        if "tickByTickData" in self._cached_data:
                            print("Found tickByTickData in JSON")
                            sample_round = next(iter(self._cached_data["tickByTickData"]))
                            print(f"Sample round: {sample_round}")
                            sample_tick = self._cached_data["tickByTickData"][sample_round][0]
                            print(f"Sample tick data: {json.dumps(sample_tick, indent=2)}")
                            print("\nData structure:")
                            print("- tickByTickData:")
                            for round_num in self._cached_data["tickByTickData"]:
                                tick_count = len(self._cached_data["tickByTickData"][round_num])
                                print(f"  Round {round_num}: {tick_count} ticks")
                                if tick_count > 0:
                                    first_tick = self._cached_data["tickByTickData"][round_num][0]["tick"]
                                    last_tick = self._cached_data["tickByTickData"][round_num][-1]["tick"]
                                    print(f"    Tick range: {first_tick} - {last_tick}")
                        else:
                            print("No tickByTickData found in JSON!")
                            print("Available keys:", list(self._cached_data.keys()))
                    
                    # Keep the file for inspection
                    print(f"JSON file available at: {output_path}")
                else:
                    print("Output file does not exist!")
                    print("Current directory contents:", os.listdir(parser_dir))
                    raise Exception("Output file not found")
            except Exception as e:
                print(f"Error processing output: {str(e)}")
                raise
    
    def get_game_state_at_tick(self, tick: int):
        """Get game state at a specific tick"""
        self._ensure_data_loaded()
        
        # Find the round that contains this tick
        tick_data = None
        # Convert round numbers from strings to integers
        tick_data_dict = {int(k): v for k, v in self._cached_data.get("tickByTickData", {}).items()}
        for round_num, round_ticks in tick_data_dict.items():
            for t in round_ticks:
                if t["tick"] == tick:
                    tick_data = t
                    break
            if tick_data:
                break
        
        if not tick_data:
            raise Exception(f"No data found for tick {tick}")
        
        # Convert tick data to game state format
        class GameState:
            def __init__(self):
                self.players = []

        class Player:
            def __init__(self):
                self.name = None
                self.position = None
                self.view_angles = None
                self.active_weapon = None
                self.last_killer = None

        class Position:
            def __init__(self, x, y, z):
                self.x = x
                self.y = y
                self.z = z

        class ViewAngles:
            def __init__(self, pitch, yaw):
                self.pitch = pitch
                self.yaw = yaw

        class Weapon:
            def __init__(self, name):
                self.name = name

        # Create game state
        game_state = GameState()
        
        # Convert each player's data
        for player_name, player_data in tick_data["players"].items():
            player = Player()
            player.name = player_name
            player.position = Position(
                player_data["position"]["x"],
                player_data["position"]["y"],
                player_data["position"]["z"]
            )
            player.view_angles = ViewAngles(
                player_data["viewAngles"]["pitch"],
                player_data["viewAngles"]["yaw"]
            )
            player.active_weapon = Weapon(player_data["activeWeapon"])
            player.last_killer = player_data.get("lastKiller")  # Get lastKiller if it exists
            game_state.players.append(player)
        
        return game_state
