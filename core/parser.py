import os
import sys
import json
import gzip
import shutil
import traceback
from typing import Dict, List, Optional

# Add import for parseToPOV
sys.path.append(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "blender"))
from parseToPOV import parse_to_pov

# Import local modules
from .demo_parser_utils import NaNEncoder
from .demo_processor import parse_demo
from .queue_manager import (
    mark_as_parsed, read_queue, write_queue,
    add_to_queue
)

# Add flag to control POV parsing
GENERATE_POV = True

class DemoParser:
    def __init__(self):
        self.stop_flag = False
        self.demo_parser = None  # Store the demoparser2 instance

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
            # Get both the output JSON and parser instance
            output_json, parser = parse_demo(dem_file_path)
            
            # Store the parser instance for POV generation
            self.demo_parser = parser
            
            # If POV generation is enabled and we have valid output
            if GENERATE_POV and output_json and self.demo_parser:
                try:
                    # Process each collection type
                    collections = [
                        ('aceKills', output_json.get('aceKills', [])),
                        ('quadKills', output_json.get('quadKills', [])),
                        ('multiKills', output_json.get('multiKills', []))
                    ]
                    
                    for collection_type, kills in collections:
                        for i, kill_sequence in enumerate(kills):
                            player = kill_sequence['player']
                            start_tick = kill_sequence['kills'][0]['killDetails']['tick']
                            end_tick = kill_sequence['kills'][-1]['killDetails']['tick']
                            
                            # Get collection number based on index in the collection
                            collection_num = i + 1
                            
                            # Call parseToPOV for this sequence
                            parse_to_pov(
                                demo=self.demo_parser,
                                start_tick=start_tick,
                                end_tick=end_tick,
                                player_name=player,
                                collection_num=collection_num
                            )
                except Exception as pov_error:
                    print(f"Error generating POV data: {str(pov_error)}")
                    traceback.print_exc()
            
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
