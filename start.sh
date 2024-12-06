#!/bin/bash

# Directory and repository URL
INSTALL_DIR="/home/josh/demofetch"
REPO_URL="https://github.com/jhohen217/demofetch.git"

# Ensure required tools are installed
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

if ! command_exists git; then
    echo "Installing git..."
    sudo apt-get update && sudo apt-get install -y git
fi

if ! command_exists python3; then
    echo "Installing Python 3..."
    sudo apt-get install -y python3 python3-pip
fi

# Clone or update the repository
cd "$INSTALL_DIR" || exit
if [ -d ".git" ]; then
    echo "Updating from git..."
    git fetch origin
    git reset --hard origin/master  # Use 'main' if your branch is 'main'
else
    echo "Cloning repository..."
    git clone "$REPO_URL" "$INSTALL_DIR"
    cd "$INSTALL_DIR" || exit
fi

# Set up Python environment
if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv
fi

echo "Activating virtual environment and installing requirements..."
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
deactivate

# Start the bot
echo "Starting bot..."
source venv/bin/activate
python3 bot.py
