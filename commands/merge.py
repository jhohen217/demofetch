import os
import glob
import logging

logger = logging.getLogger('discord_bot')

def merge_files():
    """Merge txt files from merge directory into textfiles directory"""
    # Source directories
    merge_dir = r"C:\demofetch\merge"
    textfiles_dir = r"C:\demofetch\textfiles"
    
    # Get all txt files in merge directory
    merge_files = glob.glob(os.path.join(merge_dir, "*.txt"))
    
    for merge_file_path in merge_files:
        # Get the filename without path
        filename = os.path.basename(merge_file_path)
        textfiles_path = os.path.join(textfiles_dir, filename)
        
        # Skip if corresponding file doesn't exist in textfiles directory
        if not os.path.exists(textfiles_path):
            logger.warning(f"No matching file found for {filename} in textfiles directory")
            continue
            
        # Read content from both files
        with open(merge_file_path, 'r', encoding='utf-8') as f:
            merge_content = set(line.strip() for line in f if line.strip())
            
        with open(textfiles_path, 'r', encoding='utf-8') as f:
            textfiles_content = set(line.strip() for line in f if line.strip())
            
        # Merge content and sort
        merged_content = sorted(merge_content.union(textfiles_content))
        
        # Write merged content back to textfiles directory
        with open(textfiles_path, 'w', encoding='utf-8') as f:
            f.write('\n'.join(merged_content))
            if merged_content:  # Add final newline if file is not empty
                f.write('\n')
                
        logger.info(f"Successfully merged {filename}")
        
        # Delete the processed file from merge directory
        os.remove(merge_file_path)
        logger.info(f"Deleted {filename} from merge directory")

HELP_TEXT = """
merge - Merge text files from merge directory into textfiles directory
Usage: merge
This command will:
1. Find all .txt files in C:\\demofetch\\merge
2. Merge them with matching files in C:\\demofetch\\textfiles
3. Sort content alphabetically
4. Remove source files after successful merge
"""

async def handle_message(bot, message):
    """Handle merge command"""
    if message.content.lower() != 'merge':
        return False
        
    try:
        # Check if merge directory exists and has files
        merge_dir = r"C:\demofetch\merge"
        if not os.path.exists(merge_dir):
            await bot.send_message(message.author, "Error: Merge directory does not exist")
            return True
            
        txt_files = [f for f in os.listdir(merge_dir) if f.endswith('.txt')]
        if not txt_files:
            await bot.send_message(message.author, "No .txt files found in merge directory")
            return True
            
        # Run merge operation
        merge_files()
        
        await bot.send_message(message.author, "Merge operation completed successfully")
        return True
        
    except Exception as e:
        logger.error(f"Error in merge command: {str(e)}")
        await bot.send_message(message.author, f"Error during merge: {str(e)}")
        return True

def setup(bot):
    """Optional setup function"""
    logger.info("Merge command module loaded")
