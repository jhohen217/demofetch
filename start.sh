#!/bin/bash

# Directory where the bot should be installed
INSTALL_DIR="/home/josh/demofetch/DiscordBot"
REPO_URL="https://github.com/jhohen217/demofetch.git"

# Ensure the script is running in the correct directory
mkdir -p "$INSTALL_DIR"
cd "$INSTALL_DIR" || exit  # Move to bot directory for all operations

# Function to check if a command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Install git if not present
if ! command_exists git; then
    echo "Installing git..."
    sudo apt-get update
    sudo apt-get install -y git
fi

# Install Python 3 and pip if not present
if ! command_exists python3; then
    echo "Installing Python 3..."
    sudo apt-get install -y python3 python3-pip python3-venv
fi

# Clone or update repository
if [ -d "$INSTALL_DIR/.git" ]; then
    echo "Checking for updates from GitHub..."
    git fetch origin
    LOCAL=$(git rev-parse HEAD)
    REMOTE=$(git rev-parse origin/master)
    BASE=$(git merge-base HEAD origin/master)

    if [ "$LOCAL" = "$REMOTE" ]; then
        echo "Local repository is up-to-date."
    elif [ "$LOCAL" = "$BASE" ]; then
        echo "Local repository is outdated. Pulling latest changes..."
        git reset --hard origin/master
        git pull origin master
        echo "Updating submodules..."
        git submodule update --init --recursive
    else
        echo "Local and remote branches have diverged. Forcing update..."
        git reset --hard origin/master
        git pull origin master --force
        echo "Updating submodules..."
        git submodule update --init --recursive
    fi
else
    echo "Cloning repository..."
    git clone "$REPO_URL" "$INSTALL_DIR"
    cd "$INSTALL_DIR" || exit
    echo "Initializing submodules..."
    git submodule update --init --recursive
fi

# Create virtual environment if it doesn't exist
if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv
fi

# Ensure virtual environment is activated
echo "Activating virtual environment..."
source venv/bin/activate || exit 1

# Ensure dependencies are installed
echo "Installing/updating dependencies..."
pip install --upgrade -r requirements.txt

# Check if config.json exists; if not, create from example
if [ ! -f "config.json" ] && [ -f "config.json.example" ]; then
    echo "Config file missing. Creating default config.json from example..."
    cp config.json.example config.json
    echo "Please configure 'config.json' before running the bot."
    exit 1
fi

# Set PYTHONPATH to include the project directory
export PYTHONPATH="$INSTALL_DIR:$PYTHONPATH"

# Change to bot directory and start
echo "Starting the bot..."
cd "$INSTALL_DIR" || exit
python3 DiscordBotStart.py
