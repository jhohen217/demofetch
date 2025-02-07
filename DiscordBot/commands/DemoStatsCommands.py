import os
import glob
import json
import logging

logger = logging.getLogger('discord_bot')

def _count_parsed_matches():
    """Count total number of parsed matches by reading all .txt files in parsed directory"""
    total_matches = 0
    
    # Get project directory from config
    try:
        core_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # DiscordBot directory
        config_path = os.path.join(os.path.dirname(core_dir), 'config.json')
        
        with open(config_path, 'r') as f:
            config = json.load(f)
            parsed_dir = config['project']['KillCollectionParse']
            
        if os.path.exists(parsed_dir):
            txt_files = glob.glob(os.path.join(parsed_dir, "**", "*.txt"), recursive=True)
            for file_path in txt_files:
                try:
                    with open(file_path, 'r') as f:
                        total_matches += sum(1 for line in f if line.strip())
                except Exception as e:
                    logger.error(f"Error reading {file_path}: {str(e)}")
    except Exception as e:
        logger.error(f"Error loading config: {str(e)}")
        
    return total_matches

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
            month_dirs.sort()  # Sort months alphabetically

            # Build status message
            status_parts = []
            status_parts.append("Storage Information:")
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
            status_parts.append(f"Parsed: {parsed_matches}")

            # Add per-month breakdown
            if month_dirs:
                status_parts.append("\nPer-Month Breakdown:")
                for month in month_dirs:
                    month_dir = os.path.join(textfiles_dir, month)
                    month_lower = month.lower()

                    # Count matches in each category for this month
                    ace_file = os.path.join(month_dir, f"ace_matchids_{month_lower}.txt")
                    quad_file = os.path.join(month_dir, f"quad_matchids_{month_lower}.txt")
                    match_file = os.path.join(month_dir, f"match_ids_{month_lower}.txt")
                    downloaded_file = os.path.join(month_dir, f"downloaded_{month_lower}.txt")
                    rejected_file = os.path.join(month_dir, f"rejected_{month_lower}.txt")

                    def count_lines(file_path):
                        try:
                            with open(file_path, 'r') as f:
                                return sum(1 for line in f if line.strip())
                        except:
                            return 0

                    ace_count = count_lines(ace_file)
                    quad_count = count_lines(quad_file)
                    total_count = count_lines(match_file)
                    downloaded_count = count_lines(downloaded_file)
                    rejected_count = count_lines(rejected_file)
                    undownloaded = total_count - (downloaded_count + rejected_count)

                    status_parts.append(f"\n{month}:")
                    status_parts.append(f"  Ace: {ace_count}")
                    status_parts.append(f"  Quad: {quad_count}")
                    status_parts.append(f"  Total: {total_count}")
                    status_parts.append(f"  Downloaded: {downloaded_count}")
                    status_parts.append(f"  Rejected: {rejected_count}")
                    status_parts.append(f"  Undownloaded: {undownloaded}")

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
