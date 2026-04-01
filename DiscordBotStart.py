import os
import sys
import configparser
import logging
from pathlib import Path

# Set up logging first, before any imports
logging.basicConfig(
    level=logging.DEBUG,  # Set to DEBUG level
    format='%(asctime)s - %(levelname)s - %(module)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    force=True  # Force reconfiguration of the logger
)

# Add project root and DiscordBot directory to Python path
current_dir = Path(__file__).parent.absolute()
discord_bot_dir = os.path.join(current_dir, 'DiscordBot')
if str(current_dir) not in sys.path:
    sys.path.insert(0, str(current_dir))
    logging.debug(f"Added {current_dir} to Python path")
if str(discord_bot_dir) not in sys.path:
    sys.path.insert(0, str(discord_bot_dir))
    logging.debug(f"Added {discord_bot_dir} to Python path")

logging.debug(f"Python path: {sys.path}")

# Import after path setup
from core.DiscordBot import DemoBot

def _check_config(config):
    """
    Validate that config.ini has been filled in and is not still using
    placeholder values from config.ini.example.
    Raises ValueError with a descriptive message if placeholders are found.
    """
    placeholder_markers = [
        'YOUR_DISCORD_BOT_TOKEN_HERE',
        'YOUR_FACEIT_API_KEY_HERE',
        '/path/to/',
        'C:/path/',
        'C:\\path\\',
    ]
    checks = [
        ('Keys', 'discord_token'),
        ('Keys', 'faceit_api_key'),
        ('Paths', 'project_directory'),
        ('Paths', 'textfiles_directory'),
    ]
    for section, key in checks:
        try:
            value = config.get(section, key)
            for marker in placeholder_markers:
                if marker.lower() in value.lower():
                    raise ValueError(
                        f"config.ini still contains a placeholder value for [{section}] {key}.\n"
                        f"  Value: {value}\n"
                        f"Please edit /home/josh/demofetch/config.ini with your real settings before starting."
                    )
        except configparser.NoSectionError:
            raise ValueError(f"config.ini is missing the [{section}] section.")
        except configparser.NoOptionError:
            raise ValueError(f"config.ini is missing the key '{key}' under [{section}].")


def main():
    logging.info("Starting DemoFetch application...")
    
    try:
        # Load configuration
        logging.info("Loading configuration...")
        config_path = os.path.join(current_dir, 'config.ini')
        config = configparser.ConfigParser()
        config.read(config_path)
        logging.info("Configuration loaded successfully")

        # Refuse to start if config still has placeholder / example values
        _check_config(config)
        
        # Create essential directories
        project_dir = config.get('Paths', 'project_directory')
        logging.info(f"Project directory set to: {project_dir}")
        
        os.makedirs(os.path.join(project_dir, "textfiles"), exist_ok=True)
        os.makedirs(os.path.join(project_dir, "parsed"), exist_ok=True)
        os.makedirs(os.path.join(project_dir, "parsed", "KillCollectionMaster"), exist_ok=True)
        os.makedirs(os.path.join(project_dir, "usermatches"), exist_ok=True)
        os.makedirs(os.path.join(project_dir, "userdemos"), exist_ok=True)
        logging.info("Required directories created/verified")
        
        # Initialize and run the bot with config
        logging.info("Initializing bot...")
        bot = DemoBot(config)
        
        logging.info("Starting bot...")
        bot.run(config.get('Keys', 'discord_token'))
    except FileNotFoundError:
        logging.error("config.ini not found. Please create it.")
        sys.exit(1)
    except Exception as e:
        logging.exception("Error in main:")  # This will log the full traceback
        sys.exit(1)
    finally:
        logging.info("Bot shutdown complete")
    # Note: No input() call here — allows clean exit when running as a service
    # or on headless systems (e.g. Raspberry Pi via systemd/cron).

if __name__ == "__main__":
    main()
