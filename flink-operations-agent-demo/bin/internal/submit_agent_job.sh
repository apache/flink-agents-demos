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

# Submit Flink operations agent job
# Usage: ./submit_agent_job.sh

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BIN_DIR="$(dirname "$SCRIPT_DIR")"
PROJECT_ROOT="$(dirname "$BIN_DIR")"

# Set default FLINK_HOME
if [ -z "$FLINK_HOME" ]; then
    FLINK_HOME="$PROJECT_ROOT/flink-1.20.3"
fi

# Check if FLINK_HOME exists
if [ ! -d "$FLINK_HOME" ]; then
    echo "Error: FLINK_HOME directory does not exist: $FLINK_HOME"
    exit 1
fi

echo "=========================================="
echo "Submitting Flink operations agent job"
echo "=========================================="

cd "$PROJECT_ROOT"

source venv/bin/activate
export PYTHONPATH=$(python -c 'import sysconfig; print(sysconfig.get_paths()["purelib"])')

$FLINK_HOME/bin/flink run -d \
    -pym operations_agent_main \
    -pyfs ./operations-agent-job

if [ $? -ne 0 ]; then
    echo ""
    echo "=========================================="
    echo "Error: Failed to submit operations agent job"
    echo "=========================================="
    exit 1
fi

echo ""
echo "=========================================="
echo "Operations agent job submitted successfully"
echo "=========================================="