import os
import glob
import json
import logging

# Set up logging
logger = logging.getLogger('discord_bot')
if __name__ == "__main__":
    # Configure basic logging when running as standalone
    logging.basicConfig(
        level=logging.INFO,
        format='%(message)s'
    )
    logger = logging.getLogger(__name__)

def merge_files():
    """Merge txt files from MergeMe directory into textfiles directory"""
    # Get project directory from config
    core_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # DiscordBot directory
    config_path = os.path.join(os.path.dirname(core_dir), 'config.json')
    
    with open(config_path, 'r') as f:
        config = json.load(f)
        project_dir = config['project']['directory']
        
    # Source directories
    textfiles_dir = os.path.join(project_dir, "textfiles")
    merge_dir = os.path.join(textfiles_dir, "MergeMe")
    
    # Get all txt files in merge directory
    merge_files = glob.glob(os.path.join(merge_dir, "*.txt"))
    
    if not merge_files:
        logger.info("No .txt files found in MergeMe directory")
        return
        
    merged_files = []
    skipped_files = []
    
    for merge_file_path in merge_files:
        # Get the filename without path
        filename = os.path.basename(merge_file_path)
        textfiles_path = os.path.join(textfiles_dir, filename)
        
        try:
            # Check if corresponding file exists in textfiles directory
            if not os.path.exists(textfiles_path):
                logger.warning(f"No matching file found for {filename} in textfiles directory")
                skipped_files.append(filename)
                continue
                
            # Read content from both files
            with open(merge_file_path, 'r', encoding='utf-8') as f:
                merge_content = set(line.strip() for line in f if line.strip())
                
            with open(textfiles_path, 'r', encoding='utf-8') as f:
                textfiles_content = set(line.strip() for line in f if line.strip())
                
            # Merge content and sort (reverse=True for bigger numbers at top)
            merged_content = sorted(merge_content.union(textfiles_content), reverse=True)
            
            # Write merged content back to textfiles directory
            with open(textfiles_path, 'w', encoding='utf-8') as f:
                f.write('\n'.join(merged_content))
                if merged_content:  # Add final newline if file is not empty
                    f.write('\n')
                    
            merged_files.append(filename)
            logger.info(f"Successfully merged {filename}")
            
        except Exception as e:
            logger.error(f"Error processing {filename}: {str(e)}")
            skipped_files.append(filename)
            
    # Delete all txt files from merge directory
    for merge_file_path in merge_files:
        try:
            os.remove(merge_file_path)
            logger.info(f"Deleted {os.path.basename(merge_file_path)} from MergeMe directory")
        except Exception as e:
            logger.error(f"Error deleting {os.path.basename(merge_file_path)}: {str(e)}")
            
    # Return summary
    return {
        'merged': merged_files,
        'skipped': skipped_files
    }

HELP_TEXT = """
merge - Merge text files from MergeMe directory into textfiles directory
Usage: merge
This command will:
1. Find all .txt files in textfiles/MergeMe directory
2. Merge them with matching files in textfiles directory
3. Sort content alphabetically
4. Remove source files after successful merge
"""

async def handle_message(bot, message):
    """Handle merge command"""
    if message.content.lower() != 'merge':
        return False
        
    try:
        # Get project directory from config
        core_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # DiscordBot directory
        config_path = os.path.join(os.path.dirname(core_dir), 'config.json')
        
        with open(config_path, 'r') as f:
            config = json.load(f)
            project_dir = config['project']['directory']
            
        merge_dir = os.path.join(project_dir, "textfiles", "MergeMe")
        
        # Check if merge directory exists
        if not os.path.exists(merge_dir):
            await bot.send_message(message.author, "Error: MergeMe directory does not exist")
            return True
            
        # Run merge operation
        result = merge_files()
        
        if not result:
            await bot.send_message(message.author, "No .txt files found in MergeMe directory")
            return True
            
        # Format summary message
        summary = "Merge operation completed:\n"
        if result['merged']:
            summary += f"\nMerged files:\n" + "\n".join(f"- {f}" for f in result['merged'])
        if result['skipped']:
            summary += f"\n\nSkipped files (no matching file in textfiles):\n" + "\n".join(f"- {f}" for f in result['skipped'])
            
        await bot.send_message(message.author, summary)
        return True
        
    except Exception as e:
        logger.error(f"Error in merge command: {str(e)}")
        await bot.send_message(message.author, f"Error during merge: {str(e)}")
        return True

def setup(bot):
    """Optional setup function"""
    logger.info("Merge command module loaded")

if __name__ == "__main__":
    try:
        print("Starting merge operation...")
        result = merge_files()
        
        if not result:
            print("No .txt files found in MergeMe directory")
        else:
            print("\nMerge operation completed:")
            if result['merged']:
                print("\nMerged files:")
                for f in result['merged']:
                    print(f"- {f}")
            if result['skipped']:
                print("\nSkipped files (no matching file in textfiles):")
                for f in result['skipped']:
                    print(f"- {f}")
                    
        input("\nPress Enter to exit...")  # Keep window open
    except Exception as e:
        print(f"Error during merge: {str(e)}")
        input("\nPress Enter to exit...")  # Keep window open on error
