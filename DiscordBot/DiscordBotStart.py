import os
import sys
import json
import logging
from pathlib import Path

# Set up logging first, before any imports
logging.basicConfig(
    level=logging.DEBUG,  # Set to DEBUG level
    format='%(asctime)s - %(levelname)s - %(module)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    force=True  # Force reconfiguration of the logger
)

# Add only the project root to Python path
current_dir = Path(__file__).parent.absolute()
if str(current_dir) not in sys.path:
    sys.path.insert(0, str(current_dir))
    logging.debug(f"Added {current_dir} to Python path")

logging.debug(f"Python path: {sys.path}")

# Import after path setup
from core.DiscordBot import DemoBot

def main():
    logging.info("Starting DemoFetch application...")
    
    try:
        # Load configuration
        logging.info("Loading configuration...")
        config_path = os.path.join(current_dir.parent, 'config.json')
        with open(config_path, 'r') as f:
            config = json.load(f)
        logging.info("Configuration loaded successfully")
        
        # Create essential directories
        project_dir = config['project']['directory']
        logging.info(f"Project directory set to: {project_dir}")
        
        os.makedirs(os.path.join(project_dir, "textfiles"), exist_ok=True)
        os.makedirs(os.path.join(project_dir, "parsed"), exist_ok=True)
        os.makedirs(os.path.join(project_dir, "usermatches"), exist_ok=True)
        os.makedirs(os.path.join(project_dir, "userdemos"), exist_ok=True)
        logging.info("Required directories created/verified")
        
        # Initialize and run the bot with config
        logging.info("Initializing bot...")
        bot = DemoBot(config)
        
        logging.info("Starting bot...")
        bot.run(config['discord']['token'])
    except FileNotFoundError:
        logging.error("config.json not found. Please create it from config.json.example")
        sys.exit(1)
    except json.JSONDecodeError:
        logging.error("config.json is not valid JSON")
        sys.exit(1)
    except Exception as e:
        logging.exception("Error in main:")  # This will log the full traceback
        sys.exit(1)
    finally:
        logging.info("Bot shutdown complete")

if __name__ == "__main__":
    main()
