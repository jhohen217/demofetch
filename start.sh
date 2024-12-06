#!/bin/bash

# Directory where the bot should be installed
INSTALL_DIR="/home/pi/demofetch"
REPO_URL="https://github.com/jhohen217/demofetch.git"

# Create install directory if it doesn't exist
mkdir -p "$INSTALL_DIR"
cd "$INSTALL_DIR"

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

# Install python3 and pip if not present
if ! command_exists python3; then
    echo "Installing python3..."
    sudo apt-get install -y python3 python3-pip
fi

# Clone or update repository
if [ -d .git ]; then
    echo "Updating from git..."
    git fetch
    git reset --hard origin/main
else
    echo "Cloning repository..."
    git clone "$REPO_URL" .
fi

# Create virtual environment if it doesn't exist
if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv
fi

# Activate virtual environment and install/update requirements
echo "Installing/updating dependencies..."
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt

# Check if config.json exists, if not create from example
if [ ! -f "config.json" ] && [ -f "config.json.example" ]; then
    echo "Please configure config.json before running the bot"
    cp config.json.example config.json
    exit 1
fi

# Start the bot
echo "Starting bot..."
python3 bot.py
