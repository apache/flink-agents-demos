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

# Start all services for operations-agent-demo
# Usage: ./start_all.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "============================================================"
echo "  Starting All Services for Diagnosis Agent Demo"
echo "============================================================"
echo ""

# Step 1: Start infrastructure (Kafka, Elasticsearch)
echo "[Step 1/6] Starting infrastructure services (Kafka, Elasticsearch)..."
echo "------------------------------------------------------------"
"$SCRIPT_DIR/internal/setup_infra.sh"
if [ $? -ne 0 ]; then
    echo "Error: Failed to start infrastructure services"
    exit 1
fi
echo ""

# Step 2: Setup and start Flink cluster
echo "[Step 2/6] Setting up and starting Flink cluster..."
echo "------------------------------------------------------------"
"$SCRIPT_DIR/internal/setup_flink.sh"
if [ $? -ne 0 ]; then
    echo "Error: Failed to setup and start Flink cluster"
    exit 1
fi
echo ""

# Step 3: Upload SOP documents to Elasticsearch
echo "[Step 3/6] Uploading SOP documents to Elasticsearch vector store..."
echo "------------------------------------------------------------"
"$SCRIPT_DIR/refresh_vector_store_sop.sh"
if [ $? -ne 0 ]; then
    echo "Error: Failed to upload SOP documents to Elasticsearch"
    exit 1
fi
echo ""

sleep 2

# Step 4: Submit operations agent job
echo "[Step 4/6] Submitting operations agent job..."
echo "------------------------------------------------------------"
"$SCRIPT_DIR/internal/submit_agent_job.sh"
if [ $? -ne 0 ]; then
    echo "Error: Failed to submit operations agent job"
    exit 1
fi
echo ""

sleep 2

# Step 5: Submit sample jobs
echo "[Step 5/6] Submitting sample Flink jobs..."
echo "------------------------------------------------------------"
"$SCRIPT_DIR/internal/submit_sample_jobs.sh"
if [ $? -ne 0 ]; then
    echo "Error: Failed to submit sample Flink jobs"
    exit 1
fi
echo ""

sleep 2

# Wait a moment to ensure the backpressure of Flink job is ready
echo "Waiting for jobs to reach demo state (backpressure ready)..."
WAIT_TIME=150
for ((i=1; i<=WAIT_TIME; i++)); do
    printf "\r[%3d/%d] " $i $WAIT_TIME
    # Draw progress bar
    PROGRESS=$((i * 50 / WAIT_TIME))
    printf "["
    for ((j=0; j<PROGRESS; j++)); do printf "="; done
    printf ">"
    for ((j=PROGRESS; j<50; j++)); do printf " "; done
    printf "] %3d%%" $((i * 100 / WAIT_TIME))
    sleep 1
done
printf "\n"
echo "Jobs are now in demo state!"

echo "============================================================"
echo "  All Services Started Successfully!"
echo "============================================================"
echo ""
echo "  Services running:"
echo "    - Kafka:         localhost:9092"
echo "    - Elasticsearch: localhost:9200"
echo "    - Flink Web UI:  localhost:8082"
echo ""
echo "============================================================"

# Step 6: Start auto send job info
echo "[Step 6/6] Starting auto send job info to Kafka..."
echo "------------------------------------------------------------"
echo "This will automatically send job info to Kafka every 3 minutes (180 seconds)"
echo "Total duration: 30 minutes (1800 seconds)"
echo "Press Ctrl+C to stop at any time"
echo ""

# Start in background with nohup to ensure it keeps running
nohup "$SCRIPT_DIR/internal/auto_send_job_info.sh" --duration 1800 > /tmp/auto_send_job_info.log 2>&1 &
AUTO_SEND_PID=$!

# Wait a moment to ensure the process started
sleep 2

# Check if process is still running
if ps -p $AUTO_SEND_PID > /dev/null; then
    echo "Auto send job info started successfully in background (PID: $AUTO_SEND_PID)"
    echo "Log file: /tmp/auto_send_job_info.log"
    echo ""
    echo "To stop it manually: ./bin/internal/auto_send_job_info.sh --stop"
    echo "To check status: ./bin/internal/auto_send_job_info.sh --status"
    echo "To view logs: tail -f /tmp/auto_send_job_info.log"
else
    echo "Warning: Failed to start auto send job info"
    echo "Check log file: /tmp/auto_send_job_info.log"
fi
echo ""
echo "============================================================"
echo "  Setup Complete!"
echo "============================================================"
echo ""

# Run consume_operations_record.sh in foreground so user can Ctrl+C to exit
"$SCRIPT_DIR/internal/consume_operations_record.sh"