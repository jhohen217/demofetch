import os
import glob
import json
import logging
import configparser

logger = logging.getLogger('discord_bot')

# Valid lowercase month names for sort key resolution
_MONTH_ORDER = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12
}

def _month_sort_key(folder_name: str):
    """
    Return a (year, month_number) sort key for a month folder.
    Handles both legacy plain names (e.g. "February") and MonthYY format
    (e.g. "February26").  Legacy folders sort before year-tagged ones.
    """
    name_lower = folder_name.lower()
    for m, num in _MONTH_ORDER.items():
        if name_lower == m:
            return (0, num)  # Legacy plain name
        if name_lower.startswith(m) and len(name_lower) == len(m) + 2:
            suffix = name_lower[len(m):]
            if suffix.isdigit():
                return (int(suffix), num)
    return (99, 13)  # Unknown, sort to end

def _get_config():
    """Get configuration from config.ini"""
    try:
        core_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # DiscordBot directory
        config_path = os.path.join(os.path.dirname(core_dir), 'config.ini')
        
        config = configparser.ConfigParser()
        config.read(config_path)
        return config
    except Exception as e:
        logger.error(f"Error loading config: {str(e)}")
        return {}

def _count_parsed_matches():
    """Count total number of parsed matches by reading all parsed_{month}.txt files"""
    total_matches = 0
    
    try:
        config = _get_config()
        textfiles_dir = config.get('Paths', 'textfiles_directory', fallback='')
        
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
        master_dir = config.get('Paths', 'KillCollectionMasterPath', fallback='')
        
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
            config_path = os.path.join(os.path.dirname(core_dir), 'config.ini')
            config = configparser.ConfigParser()
            config.read(config_path)
            textfiles_dir = config.get('Paths', 'textfiles_directory')

            # Get list of month directories
            month_dirs = [d for d in os.listdir(textfiles_dir)
                        if os.path.isdir(os.path.join(textfiles_dir, d))
                        and d not in ['undated', 'MergeMe']]
            
            # Sort months chronologically (oldest to newest), supports MonthYY format
            month_dirs.sort(key=_month_sort_key)

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

            # --- Build the per-month table as a SEPARATE message ---
            table_msg = None
            if month_dirs:
                # Function to count lines in a file
                def count_lines(file_path):
                    try:
                        with open(file_path, 'r') as f:
                            return sum(1 for line in f if line.strip())
                    except:
                        return 0

                # Column widths: mo:6  ace:6  quad:6  undl:7  cost:8  size:8
                W = {'mo': 6, 'ace': 6, 'quad': 6, 'undl': 7, 'cost': 8, 'size': 8}

                def row(mo, ace, quad, undl, cost, size):
                    return (
                        f"{mo.ljust(W['mo'])} | "
                        f"{ace.ljust(W['ace'])} | "
                        f"{quad.ljust(W['quad'])} | "
                        f"{undl.ljust(W['undl'])} | "
                        f"{cost.ljust(W['cost'])} | "
                        f"{size}"
                    )

                table_lines = ["Per-Month Breakdown:"]
                table_lines.append("```")
                table_lines.append(row("Month", "Ace", "Quad", "Undl", "Cost", "Size(GB)"))
                table_lines.append(row("-"*W['mo'], "-"*W['ace'], "-"*W['quad'], "-"*W['undl'], "-"*W['cost'], "-"*W['size']))

                for month in month_dirs:
                    month_dir = os.path.join(textfiles_dir, month)
                    month_lower = month.lower()
                    month_abbr = month[:3]
                    for m in _MONTH_ORDER:
                        if month_lower.startswith(m) and len(month_lower) == len(m) + 2:
                            suffix = month_lower[len(m):]
                            if suffix.isdigit():
                                month_abbr = month[:3] + suffix
                            break

                    ace_file = os.path.join(month_dir, f"ace_matchids_{month_lower}.txt")
                    quad_file = os.path.join(month_dir, f"quad_matchids_{month_lower}.txt")
                    match_file = os.path.join(month_dir, f"match_ids_{month_lower}.txt")
                    downloaded_file = os.path.join(month_dir, f"downloaded_{month_lower}.txt")
                    rejected_file = os.path.join(month_dir, f"rejected_{month_lower}.txt")

                    ace_count = count_lines(ace_file)
                    quad_count = count_lines(quad_file)
                    match_count = count_lines(match_file)
                    total_count = match_count if match_count else ace_count + quad_count
                    downloaded_count = count_lines(downloaded_file)
                    rejected_count = count_lines(rejected_file)
                    undownloaded = max(0, total_count - (downloaded_count + rejected_count))
                    if undownloaded == 0 and (downloaded_count > 0 or rejected_count > 0):
                        if total_count < downloaded_count + rejected_count:
                            total_count = downloaded_count + rejected_count

                    estimated_size_gb = ace_count * 257 / 1024
                    estimated_cost = estimated_size_gb * 0.03

                    table_lines.append(row(
                        month_abbr,
                        str(ace_count),
                        str(quad_count),
                        str(undownloaded),
                        f"${estimated_cost:.2f}",
                        f"{estimated_size_gb:.2f}"
                    ))

                table_lines.append("```")
                table_msg = "\n".join(table_lines)

            # --- Send summary (plain text, split at 1900 chars if needed) ---
            summary = "\n".join(status_parts)
            if len(summary) <= 1900:
                await bot.send_message(message.author, summary)
            else:
                lines = summary.split("\n")
                chunk = ""
                for line in lines:
                    candidate = chunk + "\n" + line if chunk else line
                    if len(candidate) > 1900:
                        await bot.send_message(message.author, chunk)
                        chunk = line
                    else:
                        chunk = candidate
                if chunk:
                    await bot.send_message(message.author, chunk)

            # --- Send table as its own message (always a clean code block) ---
            if table_msg:
                await bot.send_message(message.author, table_msg)
        except Exception as e:
            error_msg = f"An error occurred: {str(e)}"
            await bot.send_message(message.author, error_msg)
        return True

    return False

def setup(bot):
    """Required setup function for the extension"""
    return True
