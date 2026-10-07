#!/bin/bash
# Startup script for Database Monitor

# Get the directory where this script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

# Activate virtual environment
if [ ! -d "venv" ]; then
    echo "Virtual environment not found. Creating venv..."
    python3 -m venv venv
    echo "Installing dependencies..."
    source venv/bin/activate
    pip install --upgrade pip
    pip install -r requirements.txt
else
    source venv/bin/activate
fi

# Set default database path if not already set
if [ -z "$SQLITE_DB_PATH" ]; then
    if [ -f "/home/ubuntu/fix_server/securities.db" ]; then
        export SQLITE_DB_PATH="/home/ubuntu/fix_server/securities.db"
    elif [ -f "securities.db" ]; then
        export SQLITE_DB_PATH="securities.db"
    fi
fi

# Set default port if not already set
export MONITOR_PORT=${MONITOR_PORT:-8080}

echo "Starting Database Monitor..."
echo "Database: ${SQLITE_DB_PATH:-securities.db}"
echo "Port: $MONITOR_PORT"
echo "Access at: http://localhost:$MONITOR_PORT"
echo ""

# Run the application
python app.py

