import os
import glob
import json
import logging

logger = logging.getLogger('discord_bot')

def _get_config():
    """Get configuration from config.json"""
    try:
        core_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # DiscordBot directory
        config_path = os.path.join(os.path.dirname(core_dir), 'config.json')
        
        with open(config_path, 'r') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Error loading config: {str(e)}")
        return {}

def _count_parsed_matches():
    """Count total number of parsed matches by reading all parsed_{month}.txt files"""
    total_matches = 0
    
    try:
        config = _get_config()
        textfiles_dir = config.get('project', {}).get('textfiles_directory', '')
        
        if not textfiles_dir or not os.path.exists(textfiles_dir):
            logger.error(f"Textfiles directory not found: {textfiles_dir}")
            return 0
        
        # Get list of month directories
        month_dirs = [d for d in os.listdir(textfiles_dir)
                    if os.path.isdir(os.path.join(textfiles_dir, d))
                    and d not in ['undated', 'MergeMe']]
        
        # Count parsed matches in each month
        for month in month_dirs:
            month_dir = os.path.join(textfiles_dir, month)
            month_lower = month.lower()
            parsed_file = os.path.join(month_dir, f"parsed_{month_lower}.txt")
            
            if os.path.exists(parsed_file):
                try:
                    with open(parsed_file, 'r') as f:
                        total_matches += sum(1 for line in f if line.strip())
                except Exception as e:
                    logger.error(f"Error reading {parsed_file}: {str(e)}")
    except Exception as e:
        logger.error(f"Error counting parsed matches: {str(e)}")
        
    return total_matches

def _get_master_csv_stats():
    """Get parsing statistics from Master.csv files"""
    stats = {
        'total_demos': 0,
        'total_collections': 0,
        'by_month': {},
        'by_type': {}
    }
    
    try:
        config = _get_config()
        master_dir = config.get('project', {}).get('KillCollectionMasterPath', '')
        
        if not master_dir or not os.path.exists(master_dir):
            logger.error(f"KillCollectionMasterPath not found: {master_dir}")
            return stats
        
        # Find all Master.csv files
        master_files = glob.glob(os.path.join(master_dir, "*_*_Master.csv"))
        
        for file_path in master_files:
            try:
                # Extract type and month from filename
                filename = os.path.basename(file_path)
                parts = filename.split('_')
                
                if len(parts) >= 3:
                    collection_type = parts[0]
                    month = parts[1]
                    
                    # Read the header information
                    with open(file_path, 'r') as f:
                        in_manifest = False
                        demos_count = 0
                        collections_count = 0
                        
                        for line in f:
                            line = line.strip()
                            
                            if line == "[MANIFEST_INFO]":
                                in_manifest = True
                                continue
                            elif line.startswith("[") and line.endswith("]"):
                                in_manifest = False
                                continue
                            
                            if in_manifest:
                                if line.startswith("TotalDemos,"):
                                    demos_count = int(line.split(',')[1])
                                elif line.startswith("TotalCollections,"):
                                    collections_count = int(line.split(',')[1])
                    
                    # Update stats
                    stats['total_demos'] += demos_count
                    stats['total_collections'] += collections_count
                    
                    # Update by month
                    if month not in stats['by_month']:
                        stats['by_month'][month] = {
                            'total_demos': 0,
                            'total_collections': 0,
                            'by_type': {}
                        }
                    
                    stats['by_month'][month]['total_demos'] += demos_count
                    stats['by_month'][month]['total_collections'] += collections_count
                    stats['by_month'][month]['by_type'][collection_type] = {
                        'demos': demos_count,
                        'collections': collections_count
                    }
                    
                    # Update by type
                    if collection_type not in stats['by_type']:
                        stats['by_type'][collection_type] = {
                            'total_demos': 0,
                            'total_collections': 0
                        }
                    
                    stats['by_type'][collection_type]['total_demos'] += demos_count
                    stats['by_type'][collection_type]['total_collections'] += collections_count
                    
            except Exception as e:
                logger.error(f"Error reading master file {file_path}: {str(e)}")
        
    except Exception as e:
        logger.error(f"Error getting master CSV stats: {str(e)}")
    
    return stats

async def handle_message(bot, message):
    """Handle message-based commands for stats"""
    content = message.content.lower()

    if content == 'info':
        try:
            from core import (
                calculate_storage_cost,
                get_match_ids_count,
                get_downloaded_match_ids_count,
                get_rejected_match_ids_count,
                get_undownloaded_match_ids_count,
                get_category_counts
            )

            size_gb, cost, file_count = calculate_storage_cost()
            total_matches = get_match_ids_count()
            downloaded_matches = get_downloaded_match_ids_count()
            rejected_matches = get_rejected_match_ids_count()
            undownloaded_matches = get_undownloaded_match_ids_count()
            parsed_matches = _count_parsed_matches()

            # Get category counts
            category_counts = get_category_counts()

            # Get textfiles directory from config
            core_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            config_path = os.path.join(os.path.dirname(core_dir), 'config.json')
            with open(config_path, 'r') as f:
                config = json.load(f)
                textfiles_dir = config['project']['textfiles_directory']

            # Get list of month directories
            month_dirs = [d for d in os.listdir(textfiles_dir)
                        if os.path.isdir(os.path.join(textfiles_dir, d))
                        and d not in ['undated', 'MergeMe']]
            
            # Define month order for chronological sorting
            month_order = {
                "January": 1, "February": 2, "March": 3, "April": 4,
                "May": 5, "June": 6, "July": 7, "August": 8,
                "September": 9, "October": 10, "November": 11, "December": 12
            }
            
            # Sort months chronologically (oldest to newest)
            month_dirs.sort(key=lambda x: month_order.get(x, 13))  # Unknown months at the end

            # Get version information from git
            try:
                import subprocess
                git_version = subprocess.check_output(["git", "describe", "--always"], cwd=core_dir).decode().strip()
                git_branch = subprocess.check_output(["git", "rev-parse", "--abbrev-ref", "HEAD"], cwd=core_dir).decode().strip()
                git_commit = subprocess.check_output(["git", "rev-parse", "--short", "HEAD"], cwd=core_dir).decode().strip()
                git_date = subprocess.check_output(["git", "log", "-1", "--format=%cd", "--date=short"], cwd=core_dir).decode().strip()
                version_info = f"Version: {git_branch} {git_commit} ({git_date})"
            except Exception as e:
                logger.error(f"Error getting version info: {str(e)}")
                version_info = "Version: Unknown"

            # Build status message
            status_parts = []
            status_parts.append(f"DemoFetch {version_info}")
            status_parts.append("\nStorage Information:")
            status_parts.append(f"Total Size: {size_gb:.2f} GB")
            status_parts.append(f"Total Files: {file_count} demos")
            status_parts.append(f"Estimated Cost: ${cost:.2f}")
            status_parts.append("\nMatch Categories (Total):")
            status_parts.append(f"Ace: {category_counts.get('ace', 0)}")
            status_parts.append(f"Quad: {category_counts.get('quad', 0)}")
            status_parts.append(f"Unapproved: {category_counts.get('unapproved', 0)}")
            status_parts.append("\nMatch Status (Total):")
            status_parts.append(f"Total Matches: {total_matches}")
            status_parts.append(f"Downloaded: {downloaded_matches}")
            status_parts.append(f"Rejected: {rejected_matches}")
            status_parts.append(f"Undownloaded: {undownloaded_matches}")
            # Get parsing statistics from Master.csv files
            master_stats = _get_master_csv_stats()
            
            status_parts.append(f"Parsed: {parsed_matches}")
            status_parts.append(f"Parsed (from Master files): {master_stats['total_demos']} demos, {master_stats['total_collections']} collections")
            
            # Add parsing stats by type
            if master_stats['by_type']:
                status_parts.append("\nParsed Collections by Type:")
                for collection_type, type_stats in sorted(master_stats['by_type'].items()):
                    status_parts.append(f"{collection_type}: {type_stats['total_demos']} demos, {type_stats['total_collections']} collections")

            # Add per-month breakdown in a more compact format
            if month_dirs:
                status_parts.append("\nPer-Month Breakdown:")
                
                # Function to count lines in a file
                def count_lines(file_path):
                    try:
                        with open(file_path, 'r') as f:
                            return sum(1 for line in f if line.strip())
                    except:
                        return 0
                
                # Create a compact table header with the new order (Size at the end)
                status_parts.append("\nMonth      | Ace    | Quad   | DL     | Undl   | Parsed | Unparsed| Rej    | Cost($) | Size(GB) ")
                status_parts.append("-----------|--------|--------|--------|--------|--------|---------|--------|----------|----------")
                
                for month in month_dirs:
                    month_dir = os.path.join(textfiles_dir, month)
                    month_lower = month.lower()
                    month_abbr = month[:3]  # First 3 letters of month

                    # Count matches in each category for this month
                    ace_file = os.path.join(month_dir, f"ace_matchids_{month_lower}.txt")
                    quad_file = os.path.join(month_dir, f"quad_matchids_{month_lower}.txt")
                    match_file = os.path.join(month_dir, f"match_ids_{month_lower}.txt")
                    downloaded_file = os.path.join(month_dir, f"downloaded_{month_lower}.txt")
                    rejected_file = os.path.join(month_dir, f"rejected_{month_lower}.txt")

                    ace_count = count_lines(ace_file)
                    quad_count = count_lines(quad_file)
                    
                    # Calculate total from ace and quad files if match_ids file doesn't exist or is empty
                    match_count = count_lines(match_file)
                    if match_count == 0:
                        total_count = ace_count + quad_count
                    else:
                        total_count = match_count
                    downloaded_count = count_lines(downloaded_file)
                    rejected_count = count_lines(rejected_file)
                    # Ensure undownloaded is never negative
                    undownloaded = max(0, total_count - (downloaded_count + rejected_count))
                    
                    # If undownloaded is 0 but we have downloads, it means the match_ids file is incomplete
                    # In this case, set total_count to at least the sum of downloaded and rejected
                    if undownloaded == 0 and (downloaded_count > 0 or rejected_count > 0):
                        if total_count < downloaded_count + rejected_count:
                            total_count = downloaded_count + rejected_count

                    # Calculate estimated size in GB (257MB per demo) based on ACE count
                    estimated_size_gb = ace_count * 257 / 1024
                    # Calculate estimated cost ($0.03 per GB)
                    estimated_cost = estimated_size_gb * 0.03

                    # Get parsed count for this month
                    parsed_file = os.path.join(month_dir, f"parsed_{month_lower}.txt")
                    parsed_count = count_lines(parsed_file)
                    
                    # Calculate unparsed count (downloaded but not parsed)
                    unparsed_count = max(0, downloaded_count - parsed_count)
                    
                    # Format as a table row with fixed width columns in the new order (Size at the end)
                    status_parts.append(
                        f"{month_abbr.ljust(10)} | "
                        f"{str(ace_count).ljust(6)} | "
                        f"{str(quad_count).ljust(6)} | "
                        f"{str(downloaded_count).ljust(6)} | "
                        f"{str(undownloaded).ljust(6)} | "
                        f"{str(parsed_count).ljust(6)} | "
                        f"{str(unparsed_count).ljust(7)} | "
                        f"{str(rejected_count).ljust(6)} | "
                        f"${estimated_cost:.2f}".ljust(8) + " | "
                        f"{estimated_size_gb:.2f}".ljust(8)
                    )
                
                # Add parsing stats by month if available
                if master_stats['by_month']:
                    status_parts.append("\nParsed Collections by Month:")
                    
                    # Create a compact table header for parsed collections
                    status_parts.append("\nMonth      | ACE    | QUAD   | TRIPLE | MULTI  | DOUBLE | SINGLE | Demos  | Collections")
                    status_parts.append("-----------|--------|--------|--------|--------|--------|--------|--------|------------")
                    
                    # Define month order for chronological sorting
                    month_order = {
                        "January": 1, "February": 2, "March": 3, "April": 4,
                        "May": 5, "June": 6, "July": 7, "August": 8,
                        "September": 9, "October": 10, "November": 11, "December": 12
                    }
                    
                    # Sort months chronologically
                    for month in sorted(master_stats['by_month'].keys(), key=lambda x: month_order.get(x, 13)):
                        month_stats = master_stats['by_month'][month]
                        month_abbr = month[:3]  # First 3 letters of month
                        
                        # Get collections by type (not demos)
                        ace_collections = month_stats['by_type'].get('ACE', {}).get('collections', 0)
                        quad_collections = month_stats['by_type'].get('QUAD', {}).get('collections', 0)
                        triple_collections = month_stats['by_type'].get('TRIPLE', {}).get('collections', 0)
                        single_collections = month_stats['by_type'].get('SINGLE', {}).get('collections', 0)
                        double_collections = month_stats['by_type'].get('DOUBLE', {}).get('collections', 0)
                        multi_collections = month_stats['by_type'].get('MULTI', {}).get('collections', 0)
                        
                        # For December, count lines in parsed_december.txt for demos count
                        if month == "December":
                            # Get the path to the parsed_december.txt file
                            month_dir = os.path.join(textfiles_dir, month)
                            month_lower = month.lower()
                            parsed_file = os.path.join(month_dir, f"parsed_{month_lower}.txt")
                            
                            # Count the lines in the file
                            parsed_count = 0
                            if os.path.exists(parsed_file):
                                try:
                                    with open(parsed_file, 'r') as f:
                                        parsed_count = sum(1 for line in f if line.strip())
                                except Exception as e:
                                    logger.error(f"Error reading {parsed_file}: {str(e)}")
                            
                            # Use the parsed_count for the Demos field
                            demos_count = parsed_count
                        else:
                            # For other months, use the value from master_stats
                            demos_count = month_stats['total_demos']
                        
                        # Format as a table row
                        status_parts.append(
                            f"{month_abbr.ljust(10)} | "
                            f"{str(ace_collections).ljust(6)} | "
                            f"{str(quad_collections).ljust(6)} | "
                            f"{str(triple_collections).ljust(6)} | "
                            f"{str(multi_collections).ljust(6)} | "
                            f"{str(double_collections).ljust(6)} | "
                            f"{str(single_collections).ljust(6)} | "
                            f"{str(demos_count).ljust(6)} | "
                            f"{str(month_stats['total_collections'])}"
                        )

            status_msg = "\n".join(status_parts)
            await bot.send_message(message.author, status_msg)
        except Exception as e:
            error_msg = f"An error occurred: {str(e)}"
            await bot.send_message(message.author, error_msg)
        return True

    return False

def setup(bot):
    """Required setup function for the extension"""
    return True
