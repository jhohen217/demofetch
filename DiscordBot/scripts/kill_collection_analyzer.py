#!/usr/bin/env python
import os
import csv
import glob
import json
import logging
from pathlib import Path

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('kill_collection_analyzer')

def _get_config():
    """Get configuration from config.json"""
    try:
        # Navigate up from scripts directory to find config.json
        script_dir = os.path.dirname(os.path.abspath(__file__))
        project_dir = os.path.dirname(script_dir)
        config_path = os.path.join(os.path.dirname(project_dir), 'config.json')
        
        with open(config_path, 'r') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Error loading config: {str(e)}")
        return {}

def read_kill_collection_data(file_path):
    """Parse a kill collection master CSV file and extract the data"""
    data = {
        'manifest_info': {},
        'map_totals': {},
        'weapon_totals': [],
        'kill_collections': []
    }
    
    try:
        current_section = None
        with open(file_path, 'r', encoding='utf-8') as f:
            csv_reader = csv.reader(f)
            for row in csv_reader:
                if not row:
                    continue
                
                # Check for section headers
                if row[0].startswith('[') and row[0].endswith(']'):
                    current_section = row[0].strip('[]')
                    continue
                
                # Process data based on current section
                if current_section == 'MANIFEST_INFO':
                    if len(row) >= 2:
                        data['manifest_info'][row[0]] = row[1]
                elif current_section == 'MAP_TOTALS':
                    if len(row) >= 2:
                        try:
                            data['map_totals'][row[0]] = int(row[1])
                        except ValueError:
                            logger.warning(f"Invalid map total value: {row}")
                elif current_section == 'WEAPON_TOTALS':
                    if row[0] != 'Weapon':  # Skip header row
                        try:
                            weapon_data = {
                                'Weapon': row[0],
                                'Count': int(row[1]) if len(row) > 1 else 0,
                                'ExclusiveCount': int(row[2]) if len(row) > 2 else 0
                            }
                            data['weapon_totals'].append(weapon_data)
                        except (ValueError, IndexError):
                            logger.warning(f"Invalid weapon total row: {row}")
                elif current_section == 'KILL_COLLECTIONS':
                    if row[0] != 'CollectionNum':  # Skip header row
                        try:
                            # Create a dictionary with all columns
                            collection = {}
                            header = ['CollectionNum', 'TickDuration', 'MapName', 'KillerIndex', 'KillerTeam', 
                                     'StartKillTick', 'EndKillTick', 'KillerName', 'SteamID', 'DemoName', 
                                     'KillerRadius', 'VictimsRadius', 'KillerMoveDistance', 'VictimTeam', 
                                     'RoundStartTick', 'RoundEndTick', 'RoundFreezeEnd', 'Round', 'Weapons', 
                                     'WeaponsID', 'KillTicks', 'VictimsIndex', 'TickParsed']
                            
                            for i, value in enumerate(row):
                                if i < len(header):
                                    # Convert numeric values to appropriate types
                                    if header[i] in ['CollectionNum', 'TickDuration', 'KillerIndex', 'StartKillTick', 
                                                   'EndKillTick', 'Round', 'RoundStartTick', 'RoundEndTick', 
                                                   'RoundFreezeEnd', 'TickParsed']:
                                        try:
                                            collection[header[i]] = int(value) if value.isdigit() else 0
                                        except (ValueError, TypeError):
                                            collection[header[i]] = 0
                                    elif header[i] in ['KillerRadius', 'VictimsRadius', 'KillerMoveDistance']:
                                        try:
                                            collection[header[i]] = float(value) if value and value != 'nan' else 0.0
                                        except (ValueError, TypeError):
                                            collection[header[i]] = 0.0
                                    else:
                                        collection[header[i]] = value
                            
                            # Only add if TickDuration is valid
                            if 'TickDuration' in collection and collection['TickDuration'] > 0:
                                data['kill_collections'].append(collection)
                        except Exception as e:
                            logger.warning(f"Error processing collection row: {row}, Error: {str(e)}")
        
        logger.info(f"Successfully read {len(data['kill_collections'])} collections from {file_path}")
        return data
    except Exception as e:
        logger.error(f"Error reading file {file_path}: {str(e)}")
        return data

def filter_collections(data, collection_type=None, month=None):
    """Filter collections based on type and month"""
    # The data is already filtered by type and month based on the file we read
    # This function is included for future extensibility
    return data

def calculate_average_tick_duration(collections):
    """Calculate average TickDuration and convert to time"""
    if not collections:
        return {
            'average_ticks': 0,
            'average_seconds': 0,
            'total_collections': 0
        }
    
    total_ticks = sum(col.get('TickDuration', 0) for col in collections)
    count = len(collections)
    average_ticks = total_ticks / count if count > 0 else 0
    
    # Convert to seconds (64 ticks = 1 second)
    average_seconds = average_ticks / 64
    
    return {
        'average_ticks': average_ticks,
        'average_seconds': average_seconds,
        'total_collections': count
    }

def format_time(seconds):
    """Format seconds into minutes:seconds.milliseconds"""
    minutes = int(seconds // 60)
    remaining_seconds = seconds % 60
    return f"{minutes}:{remaining_seconds:.3f}"

def analyze_kill_collections(collection_type=None, month=None):
    """Main function to analyze kill collections based on parameters"""
    try:
        config = _get_config()
        master_dir = config.get('project', {}).get('KillCollectionMasterPath', '')
        
        if not master_dir or not os.path.exists(master_dir):
            logger.error(f"KillCollectionMasterPath not found: {master_dir}")
            return "Error: Could not find kill collection master directory."
        
        # Determine which files to process
        file_pattern = []
        
        # Get all master files first
        all_master_files = glob.glob(os.path.join(master_dir, "*_*_Master.csv"))
        
        if collection_type and month:
            # Filter for specific type and month
            filtered_files = [f for f in all_master_files 
                             if os.path.basename(f).startswith(f"{collection_type}_") 
                             and f"_{month}_" in os.path.basename(f)]
            file_pattern = filtered_files
        elif collection_type:
            # Filter for specific type
            filtered_files = [f for f in all_master_files 
                             if os.path.basename(f).startswith(f"{collection_type}_")]
            file_pattern = filtered_files
        elif month:
            # Filter for specific month (case-insensitive)
            month_lower = month.lower()
            filtered_files = []
            
            # Debug: Print all master files
            logger.info(f"All master files: {[os.path.basename(f) for f in all_master_files]}")
            
            for f in all_master_files:
                basename = os.path.basename(f).lower()
                logger.info(f"Checking file: {basename} for month: {month_lower}")
                
                # Check if the month is in the filename (more permissive matching)
                # Use a more direct approach to match the month part of the filename
                parts = basename.split('_')
                if len(parts) >= 2 and parts[1].lower() == month_lower:
                    logger.info(f"Found match: {basename}")
                    filtered_files.append(f)
            
            file_pattern = filtered_files
        else:
            # All types and months
            file_pattern = all_master_files
        
        # Use the filtered files
        all_files = file_pattern
        
        if not all_files:
            return f"No kill collection data found for the specified criteria."
            
        # Log the files being processed
        logger.info(f"Processing {len(all_files)} files: {[os.path.basename(f) for f in all_files]}")
        
        # Redirect logging to a string buffer for the duration of this function
        # This prevents log messages from being treated as errors
        import io
        log_capture = io.StringIO()
        log_handler = logging.StreamHandler(log_capture)
        log_handler.setLevel(logging.INFO)
        logger.addHandler(log_handler)
    
        # Process each file
        results = []
        all_collections = []
        
        for file_path in all_files:
            try:
                # Extract type and month from filename
                filename = os.path.basename(file_path)
                parts = filename.split('_')
                
                if len(parts) >= 3:
                    file_type = parts[0]
                    file_month = parts[1]
                    
                    # Read and filter data
                    data = read_kill_collection_data(file_path)
                    filtered_data = filter_collections(data, file_type, file_month)
                    
                    # Calculate statistics
                    stats = calculate_average_tick_duration(filtered_data['kill_collections'])
                    
                    # Add to results
                    results.append({
                        'type': file_type,
                        'month': file_month,
                        'stats': stats
                    })
                    
                    # Add collections to all_collections for overall average
                    all_collections.extend(filtered_data['kill_collections'])
            except Exception as e:
                logger.error(f"Error processing file {file_path}: {str(e)}")
                logger.exception("Full traceback:")
    
        # Calculate overall average
        overall_stats = calculate_average_tick_duration(all_collections)
        
        # Format results
        output = []
        
        # Add header
        if collection_type and month:
            output.append(f"Kill Collection Analysis for {collection_type} in {month}")
        elif collection_type:
            output.append(f"Kill Collection Analysis for {collection_type} (All Months)")
        elif month:
            output.append(f"Kill Collection Analysis for {month} (All Types)")
        else:
            output.append(f"Kill Collection Analysis (All Types and Months)")
        
        output.append("")
        
        # Add overall statistics
        output.append(f"Overall Statistics:")
        output.append(f"Total Collections: {overall_stats['total_collections']}")
        output.append(f"Average TickDuration: {overall_stats['average_ticks']:.2f} ticks")
        output.append(f"Average Time: {format_time(overall_stats['average_seconds'])} (min:sec)")
        output.append("")
        
        # Add individual file statistics
        if len(results) > 1:
            output.append(f"Breakdown by Type/Month:")
            for result in results:
                output.append(f"  {result['type']} in {result['month']}:")
                output.append(f"    Collections: {result['stats']['total_collections']}")
                output.append(f"    Average TickDuration: {result['stats']['average_ticks']:.2f} ticks")
                output.append(f"    Average Time: {format_time(result['stats']['average_seconds'])} (min:sec)")
        
        # Remove the log handler and restore normal logging
        logger.removeHandler(log_handler)
        
        return "\n".join(output)
    except Exception as e:
        logger.error(f"Error in analyze_kill_collections: {str(e)}")
        logger.exception("Full traceback:")
        return f"Error analyzing kill collections: {str(e)}"

if __name__ == "__main__":
    import sys
    
    try:
        # Parse command line arguments
        collection_type = None
        month = None
        
        if len(sys.argv) > 1:
            collection_type = sys.argv[1].upper() if sys.argv[1].lower() != "all" else None
        
        if len(sys.argv) > 2:
            month = sys.argv[2].capitalize() if sys.argv[2].lower() != "all" else None
        
        # Run analysis
        result = analyze_kill_collections(collection_type, month)
        print(result)
    except Exception as e:
        logger.error(f"Error in main: {str(e)}")
        logger.exception("Full traceback:")
        print(f"Error analyzing kill collections: {str(e)}")
        sys.exit(1)
