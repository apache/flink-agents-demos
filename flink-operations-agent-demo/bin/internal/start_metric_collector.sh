#!/bin/bash
################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN_DIR="$(dirname "$SCRIPT_DIR")"
PROJECT_DIR="$(dirname "$BIN_DIR")"

# Default values
INTERVAL=5
BASE_URL="http://localhost:8082"
PID_FILE="/tmp/metric_collector.pid"
BACKGROUND=false

# Cleanup function for graceful shutdown
cleanup() {
    echo ""
    echo "Received shutdown signal, stopping metric collector..."
    rm -f "$PID_FILE"
    exit 0
}

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --interval)
            INTERVAL="$2"
            shift 2
            ;;
        --base-url)
            BASE_URL="$2"
            shift 2
            ;;
        --background)
            BACKGROUND=true
            shift
            ;;
        --stop)
            if [ -f "$PID_FILE" ]; then
                PID=$(cat "$PID_FILE")
                if kill -0 "$PID" 2>/dev/null; then
                    echo "Stopping metric collector (PID: $PID)..."
                    kill "$PID"
                    # Wait for process to terminate
                    for i in {1..10}; do
                        if ! kill -0 "$PID" 2>/dev/null; then
                            break
                        fi
                        sleep 0.5
                    done
                    rm -f "$PID_FILE"
                    echo "Metric collector stopped."
                else
                    echo "Process $PID is not running."
                    rm -f "$PID_FILE"
                fi
            else
                echo "PID file not found. Metric collector may not be running."
            fi
            exit 0
            ;;
        --status)
            if [ -f "$PID_FILE" ]; then
                PID=$(cat "$PID_FILE")
                if kill -0 "$PID" 2>/dev/null; then
                    echo "Metric collector is running (PID: $PID)"
                    exit 0
                else
                    echo "PID file exists but process $PID is not running."
                    rm -f "$PID_FILE"
                    exit 1
                fi
            else
                echo "Metric collector is not running."
                exit 1
            fi
            ;;
        --help)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --interval <seconds>  Collection interval (default: 5)"
            echo "  --base-url <url>      Flink REST API URL (default: http://localhost:8082)"
            echo "  --background          Run in background mode"
            echo "  --stop                Stop the running metric collector"
            echo "  --status              Check if metric collector is running"
            echo "  --help                Show this help message"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Check if already running
if [ -f "$PID_FILE" ]; then
    EXISTING_PID=$(cat "$PID_FILE")
    if kill -0 "$EXISTING_PID" 2>/dev/null; then
        echo "Error: Metric collector is already running (PID: $EXISTING_PID)"
        echo "Use '$0 --stop' to stop it first, or '$0 --status' to check status."
        exit 1
    else
        # Stale PID file, remove it
        rm -f "$PID_FILE"
    fi
fi

echo "====================================================="
echo "  Starting Flink Metric Collector"
echo "====================================================="

# Set environment variable for metric history directory
export METRIC_HISTORY_DIR="$PROJECT_DIR/tmp/metric_history"

# Create the directory if it doesn't exist
mkdir -p "$METRIC_HISTORY_DIR"

echo "  Metric history dir: $METRIC_HISTORY_DIR"
echo "  Flink REST API:        $BASE_URL"
echo "  Collection interval:   ${INTERVAL}s"
echo "====================================================="
echo ""

# Activate virtual environment if exists
if [ -f "$PROJECT_DIR/venv/bin/activate" ]; then
    source "$PROJECT_DIR/venv/bin/activate"
fi

if [ "$BACKGROUND" = true ]; then
    # Background mode
    echo "Starting metric collector in background..."
    nohup python "$SCRIPT_DIR/metric_collector.py" \
        --base-url "$BASE_URL" \
        --interval "$INTERVAL" > /tmp/metric_collector.log 2>&1 &

    COLLECTOR_PID=$!
    echo $COLLECTOR_PID > "$PID_FILE"
    echo "Metric collector started in background (PID: $COLLECTOR_PID)"
    echo "Log file: /tmp/metric_collector.log"
    echo "To stop: $0 --stop"
else
    # Foreground mode - register signal handlers for graceful shutdown
    trap cleanup SIGINT SIGTERM

    echo "Starting metric collector in foreground..."
    echo "Press Ctrl+C to stop"
    echo ""

    # Save PID for status check
    echo $$ > "$PID_FILE"

    # Run in foreground, exec replaces the shell process with python
    exec python "$SCRIPT_DIR/metric_collector.py" \
        --base-url "$BASE_URL" \
        --interval "$INTERVAL"
fi
