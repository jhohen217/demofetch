import os
import glob
import logging

logger = logging.getLogger('discord_bot')

def _count_parsed_matches():
    """Count total number of parsed matches by reading all .txt files in parsed directory"""
    import json
    total_matches = 0
    
    # Get project directory from config
    try:
        core_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # DiscordBot directory
        config_path = os.path.join(os.path.dirname(core_dir), 'config.json')
        
        with open(config_path, 'r') as f:
            config = json.load(f)
            project_dir = config['project']['directory']
            parsed_dir = os.path.join(project_dir, "parsed")
            
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
            
            status_msg = (
                f"Storage Information:\n"
                f"Total Size: {size_gb:.2f} GB\n"
                f"Total Files: {file_count} demos\n"
                f"Estimated Cost: ${cost:.2f}\n\n"
                f"Match Categories:\n"
                f"Ace: {category_counts.get('ace', 0)}\n"
                f"Quad: {category_counts.get('quad', 0)}\n"
                f"Unapproved: {category_counts.get('unapproved', 0)}\n\n"
                f"Match Status:\n"
                f"Total Matches: {total_matches}\n"
                f"Downloaded: {downloaded_matches}\n"
                f"Rejected: {rejected_matches}\n"
                f"Undownloaded: {undownloaded_matches}\n"
                f"Parsed: {parsed_matches}"
            )
    
            await bot.send_message(message.author, status_msg)
            return True
                
        except Exception as e:
            error_msg = f"An error occurred: {str(e)}"
            await bot.send_message(message.author, error_msg)
            return True
            
    return False

def setup(bot):
    """Required setup function for the extension"""
    return True
