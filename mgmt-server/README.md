# Database Monitor

A read-only web interface to monitor kdb+ database tables without interfering with the `fix_server` application.

## Features

- 📊 View all tables in the kdb+ database
- 🔍 Browse table schemas and column information
- 📄 Paginated table data viewing
- 🎨 Modern, responsive web interface
- 🔒 Read-only access (no modifications to database)
- ⚡ Lightweight and fast
- 🕐 Automatic timestamp conversion for kdb+ time types

## Setup

1. Create and activate virtual environment:
```bash
python3 -m venv venv
source venv/bin/activate
```

2. Install Python dependencies:
```bash
pip install -r requirements.txt
```

3. Configure kdb+ connection (optional):
```bash
export KDB_HOST="localhost"
export KDB_PORT=5001
export MONITOR_PORT=8080
```

4. Run the server:
```bash
./start.sh
```

Or manually:
```bash
source venv/bin/activate
python app.py
```

Or using uvicorn directly:
```bash
source venv/bin/activate
uvicorn app:app --host 0.0.0.0 --port 8080
```

**Note**: The `start.sh` script will automatically create and activate the virtual environment if it doesn't exist.

**Important**: 
- Make sure your kdb+ server is running and accessible at the configured host and port before starting the monitor.
- The application will try to use qpython if available, otherwise it will attempt to use the `q` command-line tool.
- For best results, install qpython: `pip install qpython` (note: may have compatibility issues with newer Python/numpy versions)

## Configuration

The application uses environment variables for configuration:

- `KDB_HOST`: kdb+ server hostname (default: `localhost`)
- `KDB_PORT`: kdb+ server port (default: `5001`)
- `MONITOR_PORT`: Port to run the web server on (default: `8080`)

## Usage

1. Start the monitoring server
2. Open your browser and navigate to `http://localhost:8080`
3. Browse tables, view schemas, and inspect data

## Logging

The application uses separate log files for better organization:

- **`logs/app.log`** - General application logs
- **`logs/access.log`** - HTTP request/response logs
- **`logs/error.log`** - Error and exception logs
- **`logs/kdb.log`** - kdb+ database query and connection logs

All log files are automatically rotated when they reach 10MB, keeping the 5 most recent backups (10 for error logs).

## Architecture

- **Backend**: FastAPI (Python)
- **Frontend**: Vanilla HTML/CSS/JavaScript
- **Database**: kdb+ (read-only access via IPC)
- **Logging**: Structured logging with separate files
- **Port**: 8080 (configurable, separate from fix_server)

## Security Notes

- This is a read-only interface - no write operations are performed
- Runs on a separate port from fix_server to avoid interference
- SQL injection protection via parameterized queries
- Table name validation before queries

