import os
import gzip
import shutil
import argparse
import json
from typing import Optional, Dict, Tuple
from core.parser import DemoParser
from core.demo_processor import parse_demo
from core.demo_parser_utils import NaNEncoder
from core.fast_parser import FastDemoParserWrapper

class DemoParserWrapper:
    """Wrapper class to add required attributes for POV generation"""
    def __init__(self, demo_parser, map_name, demo_file):
        self.parser = demo_parser
        self.map_name = map_name
        self.demo_file = demo_file
        
    def get_game_state_at_tick(self, tick):
        """Get game state at a specific tick"""
        print(f"Getting game state for tick {tick}")
        
        # Properties we want to track
        wanted_props = [
            "X", "Y", "Z",  # Position
            "pitch", "yaw",  # View angles
            "active_weapon",  # Current weapon
            "player_name",  # Player name
            "team_num"  # Team
        ]
        
        try:
            # Parse the specific tick
            print(f"Parsing tick data...")
            tick_data = self.parser.parse_ticks(wanted_props, ticks=[tick])
            print(f"Got tick data with {len(tick_data)} rows")
            
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
            
            # Process each player's data
            print("Processing player data...")
            for row in tick_data.itertuples():
                player = Player()
                player.name = getattr(row, 'player_name', 'unknown')
                player.position = Position(
                    getattr(row, 'X', 0),
                    getattr(row, 'Y', 0),
                    getattr(row, 'Z', 0)
                )
                player.view_angles = ViewAngles(
                    getattr(row, 'pitch', 0),
                    getattr(row, 'yaw', 0)
                )
                player.active_weapon = Weapon(getattr(row, 'active_weapon', 'unknown'))
                game_state.players.append(player)
            
            print(f"Created game state with {len(game_state.players)} players")
            return game_state
            
        except Exception as e:
            print(f"Error getting game state at tick {tick}: {str(e)}")
            import traceback
            traceback.print_exc()
            raise

async def async_main():
    parser = argparse.ArgumentParser(description='Parse CS:GO demo files (.dem or .dem.gz)')
    parser.add_argument('demo_path', help='Path to the demo file (.dem or .dem.gz)')
    parser.add_argument('--fast', action='store_true', help='Use fast C# parser implementation')
    args = parser.parse_args()

    if not os.path.exists(args.demo_path):
        print(f"Error: File not found: {args.demo_path}")
        return

    if not (args.demo_path.endswith('.dem') or args.demo_path.endswith('.dem.gz')):
        print("Error: File must be either .dem or .dem.gz")
        return

    # Get absolute paths
    demo_abs_path = os.path.abspath(args.demo_path)
    base_dir = os.path.dirname(os.path.dirname(demo_abs_path))
    match_id = os.path.basename(demo_abs_path).replace('.dem.gz', '').replace('.dem', '')

    print(f"Processing demo: {match_id}")
    print(f"Demo path: {demo_abs_path}")
    print(f"Base directory: {base_dir}")

    # Handle .dem.gz files
    needs_cleanup = False
    dem_path = demo_abs_path
    if demo_abs_path.endswith('.dem.gz'):
        dem_path = demo_abs_path[:-3]  # Remove .gz extension
        if not os.path.exists(dem_path):
            print(f"Extracting {demo_abs_path}...")
            with gzip.open(demo_abs_path, 'rb') as f_in:
                with open(dem_path, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
            needs_cleanup = True

    try:
        # Parse the demo using the core demo processor
        print("Parsing demo file...")
        output_json, demo_parser = parse_demo(dem_path)

        if output_json:
            # Create output directory
            parsed_dir = os.path.join(base_dir, "parsed", "test")
            os.makedirs(parsed_dir, exist_ok=True)

            # Save parsed data
            output_path = os.path.join(parsed_dir, f"{match_id}.json")
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(output_json, f, indent=4, cls=NaNEncoder)
            print(f"\nParsed data saved to: {output_path}")

            # Create wrapper with required attributes for POV generation
            if demo_parser:
                print("Creating parser wrapper...")
                if args.fast:
                    wrapped_parser = FastDemoParserWrapper(
                        demo_parser=demo_parser,
                        map_name=output_json['demoInfo']['mapName'],
                        demo_file=dem_path
                    )
                else:
                    wrapped_parser = DemoParserWrapper(
                        demo_parser=demo_parser,
                        map_name=output_json['demoInfo']['mapName'],
                        demo_file=dem_path
                    )

                # Process POV data for each collection
                collections = [
                    ('aceKills', output_json.get('aceKills', [])),
                    ('quadKills', output_json.get('quadKills', [])),
                    ('multiKills', output_json.get('multiKills', []))
                ]
                
                print("Processing kill collections...")
                for collection_type, kills in collections:
                    print(f"\nProcessing {collection_type}...")
                    for i, kill_sequence in enumerate(kills):
                        player = kill_sequence['player']
                        start_tick = kill_sequence['kills'][0]['killDetails']['tick']
                        end_tick = kill_sequence['kills'][-1]['killDetails']['tick']
                        collection_num = i + 1
                        
                        print(f"Processing {collection_type} #{collection_num} for player {player}")
                        print(f"Tick range: {start_tick} - {end_tick}")
                        
                        try:
                            from blender.parseToPOV import parse_to_pov
                            parse_to_pov(
                                demo=wrapped_parser,
                                start_tick=start_tick,
                                end_tick=end_tick,
                                player_name=player,
                                collection_num=collection_num
                            )
                            print(f"Generated POV data for {collection_type} #{collection_num}")
                        except Exception as pov_error:
                            print(f"Error generating POV data: {str(pov_error)}")
                            import traceback
                            traceback.print_exc()

            # Print summary
            print("\nDemo Summary:")
            print(f"Map: {output_json['demoInfo']['mapName']}")
            print(f"Date: {output_json['demoInfo']['date']}")
            print(f"Ace Kills: {output_json['demoInfo']['aceKills']}")
            print(f"Quad Kills: {output_json['demoInfo']['quadKills']}")
            print(f"Triple Kills: {output_json['demoInfo']['tripleKills']}")
            print(f"Multi Kills: {output_json['demoInfo']['multiKills']}")
        else:
            print("\nNo notable kills found in demo.")

    except Exception as e:
        print(f"Error processing demo: {str(e)}")
        import traceback
        traceback.print_exc()

    finally:
        # Clean up extracted file if needed
        if needs_cleanup and os.path.exists(dem_path):
            os.remove(dem_path)
            print(f"Cleaned up extracted file: {dem_path}")

def main():
    import asyncio
    asyncio.run(async_main())

if __name__ == "__main__":
    main()
