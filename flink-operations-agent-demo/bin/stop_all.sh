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

# Stop all services for diagnosis-agent-demo
# Usage: ./stop_all.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

echo "============================================================"
echo "  Stopping All Services for Diagnosis Agent Demo"
echo "============================================================"
echo ""

# Step 1: Stop watermark collector
echo "[Step 1/3] Stopping watermark collector..."
echo "------------------------------------------------------------"
"$SCRIPT_DIR/internal/start_metric_collector.sh" --stop
if [ $? -ne 0 ]; then
    echo "Warning: Failed to stop watermark collector (may not be running)"
fi
echo ""

# Step 2: Stop auto-send job info
echo "[Step 2/3] Stopping auto-send job info..."
echo "------------------------------------------------------------"
"$SCRIPT_DIR/internal/auto_send_job_info.sh" --stop
if [ $? -ne 0 ]; then
    echo "Warning: Failed to stop auto-send job info (may not be running)"
fi
echo ""

# Step 3: Stop Flink cluster
echo "[Step 3/3] Stopping Flink cluster..."
echo "------------------------------------------------------------"
if [ -d "$PROJECT_DIR/flink-1.20.3" ]; then
    echo "Stopping Flink cluster..."
    "$PROJECT_DIR/flink-1.20.3/bin/stop-cluster.sh"
    if [ $? -ne 0 ]; then
        echo "Warning: Failed to stop Flink cluster"
    fi
    
    echo "Stopping TaskManager..."
    "$PROJECT_DIR/flink-1.20.3/bin/taskmanager.sh" stop
    if [ $? -ne 0 ]; then
        echo "Warning: Failed to stop TaskManager"
    fi
    echo "Flink cluster stopped"
else
    echo "Warning: Flink directory not found ($PROJECT_DIR/flink-1.20.3)"
fi
echo ""

echo "============================================================"
echo "  All Services Stopped!"
echo "============================================================"
echo ""
echo "  To start services again, run: ./bin/start_all.sh"
echo "============================================================"
