import os
import json
import logging
import subprocess
import asyncio
from datetime import datetime

logger = logging.getLogger('discord_bot')

def get_month_files(month: str):
    """Get file paths for a specific month"""
    # Load configuration from project root
    core_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # DiscordBot directory
    config_path = os.path.join(os.path.dirname(core_dir), 'config.json')
    
    with open(config_path, 'r') as f:
        config = json.load(f)
    
    textfiles_dir = config['project']['textfiles_directory']
    month_dir = os.path.join(textfiles_dir, month)
    month_lower = month.lower()
    
    return {
        'dir': month_dir,
        'parse_queue': os.path.join(month_dir, f'parse_queue_{month_lower}.txt'),
        'parsed': os.path.join(month_dir, f'parsed_{month_lower}.txt')
    }

async def handle_message(bot, message):
    """Handle message-based parser commands"""
    content = message.content.lower()
    args = content.split()
    command = args[0] if args else ""

    if command == 'parse':
        if len(args) != 2:
            await bot.send_message(message.author, "Usage: parse <demos_directory>")
            return True

        demos_dir = args[1]
        if not os.path.isdir(demos_dir):
            await bot.send_message(message.author, f"Directory not found: {demos_dir}")
            return True

        # Get current month directory and files
        current_month = datetime.now().strftime("%B")  # e.g., "February"
        files = get_month_files(current_month)
        os.makedirs(files['dir'], exist_ok=True)

        try:
            # Get project root and path to StartCollectionsParse.py
            core_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            project_root = os.path.dirname(core_dir)
            parser_script = os.path.join(
                project_root,
                "CSharpParser", "demofile-net", "examples",
                "DemoFile.Example.FastParser", "StartCollectionsParse.py"
            )

            # Create files if they don't exist
            for file_path in [files['parse_queue'], files['parsed']]:
                if not os.path.exists(file_path):
                    with open(file_path, 'w') as f:
                        pass
            
            # Run the C# parser
            result = await asyncio.to_thread(
                subprocess.run,
                ["python", parser_script, demos_dir],
                capture_output=True,
                text=True
            )
            
            if result.returncode == 0:
                response = "Parsing completed successfully"
                if result.stdout:
                    response += f"\nOutput:\n{result.stdout}"
            else:
                response = f"Parser error:\n{result.stderr}"
            
            await bot.send_message(message.author, response)
            return True
            
        except Exception as e:
            error_msg = f"An error occurred while parsing: {str(e)}"
            await bot.send_message(message.author, error_msg)
            return True

    return False

def setup(bot):
    """Required setup function for the extension"""
    return True
