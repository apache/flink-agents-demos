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
#  limitations under the License.
################################################################################

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN_DIR="$(dirname "$SCRIPT_DIR")"
PROJECT_DIR="$(dirname "$BIN_DIR")"

# Default values
DEFAULT_BASE_URL="http://localhost:8082"
DEFAULT_INTERVAL=180
DEFAULT_DURATION=1800
DEFAULT_PID_FILE="/tmp/auto_send_job_info.pid"

# Parse command line arguments
ACTION="run"
BASE_URL="$DEFAULT_BASE_URL"
INTERVAL="$DEFAULT_INTERVAL"
DURATION="$DEFAULT_DURATION"
PID_FILE="$DEFAULT_PID_FILE"
MAX_MESSAGES=""

while [[ $# -gt 0 ]]; do
    case $1 in
        --stop)
            ACTION="stop"
            shift
            ;;
        --status)
            ACTION="status"
            shift
            ;;
        --base-url)
            BASE_URL="$2"
            shift 2
            ;;
        --interval)
            INTERVAL="$2"
            shift 2
            ;;
        --duration)
            DURATION="$2"
            shift 2
            ;;
        --pid-file)
            PID_FILE="$2"
            shift 2
            ;;
        --max-messages)
            MAX_MESSAGES="$2"
            shift 2
            ;;
        --help)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Actions:"
            echo "  (default)           Run the auto send job info script"
            echo "  --stop              Stop the running process"
            echo "  --status            Check if the process is running"
            echo ""
            echo "Options:"
            echo "  --base-url <url>    Flink REST API URL (default: $DEFAULT_BASE_URL)"
            echo "  --interval <sec>    Send interval between jobs in seconds (default: $DEFAULT_INTERVAL)"
            echo "  --duration <sec>    Total duration in seconds (default: $DEFAULT_DURATION)"
            echo "  --pid-file <path>   PID file path (default: $DEFAULT_PID_FILE)"
            echo "  --max-messages <n>  Maximum number of messages to send (default: unlimited)"
            echo "  --help              Show this help message"
            echo ""
            echo "Examples:"
            echo "  $0                                    # Run with defaults"
            echo "  $0 --base-url http://localhost:8082   # Custom Flink URL"
            echo "  $0 --max-messages 10                  # Send 10 messages then stop"
            echo "  $0 --stop                             # Stop running process"
            echo "  $0 --status                           # Check status"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

# Activate virtual environment if exists
if [ -f "$PROJECT_DIR/venv/bin/activate" ]; then
    source "$PROJECT_DIR/venv/bin/activate"
fi

# Execute action
case $ACTION in
    stop)
        echo "Stopping auto_send_job_info..."
        python3 "$SCRIPT_DIR/auto_send_job_info.py" --stop --pid-file "$PID_FILE"
        ;;
    status)
        echo "Checking auto_send_job_info status..."
        python3 "$SCRIPT_DIR/auto_send_job_info.py" --status --pid-file "$PID_FILE"
        ;;
    run)
        echo "====================================================="
        echo "  Auto Send Job Info to Kafka"
        echo "====================================================="
        echo "  Flink REST API: $BASE_URL"
        echo "  Send interval:  ${INTERVAL}s"
        echo "  Duration:       ${DURATION}s ($(($DURATION / 60)) minutes)"
        if [ -n "$MAX_MESSAGES" ]; then
            echo "  Max messages:   $MAX_MESSAGES"
        fi
        echo "  PID file:       $PID_FILE"
        echo "====================================================="
        echo ""

        # Build command arguments
        CMD_ARGS="--base-url $BASE_URL --interval $INTERVAL --duration $DURATION --pid-file $PID_FILE"
        if [ -n "$MAX_MESSAGES" ]; then
            CMD_ARGS="$CMD_ARGS --max-messages $MAX_MESSAGES"
        fi

        # Run the Python script
        python3 "$SCRIPT_DIR/auto_send_job_info.py" $CMD_ARGS
        ;;
esac