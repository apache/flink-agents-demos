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

# Number of TaskManagers to start
NUM_TASK_MANAGERS=2

echo "====================================================="
echo "  Starting Flink Standalone Cluster With Flink-Agents"
echo "====================================================="
echo "  Number of TaskManagers: $NUM_TASK_MANAGERS"
echo "====================================================="

cd "$PROJECT_DIR"

if [ ! -d "flink-1.20.3" ]; then
    # Download Flink 1.20.3 (skip if already exists)
    if [ ! -f "flink-1.20.3-bin-scala_2.12.tgz" ]; then
        curl -LO https://archive.apache.org/dist/flink/flink-1.20.3/flink-1.20.3-bin-scala_2.12.tgz
    else
        echo "flink-1.20.3-bin-scala_2.12.tgz already exists, skipping download"
    fi

    # Extract the archive
    tar -xzf flink-1.20.3-bin-scala_2.12.tgz
else
    echo "flink-1.20.3 already exists, skipping setup"
fi

# Set FLINK_HOME environment variable
export FLINK_HOME=$(pwd)/flink-1.20.3

# Copy the flink-python JAR from opt to lib (required for PyFlink)
cp $FLINK_HOME/opt/flink-python-1.20.3.jar $FLINK_HOME/lib/

# Copy custom Flink config to Flink conf directory as config.yaml
cp $SCRIPT_DIR/flink_config.yaml $FLINK_HOME/conf/config.yaml

# Automatically set agent.baseLogDir to FLINK_HOME/log
if [[ "$OSTYPE" == "darwin"* ]]; then
    sed -i '' "s|baseLogDir: EVENT_LOG_DIR_PLACEHOLDER|baseLogDir: $FLINK_HOME/log|" "$FLINK_HOME/conf/config.yaml"
else
    sed -i "s|baseLogDir: EVENT_LOG_DIR_PLACEHOLDER|baseLogDir: $FLINK_HOME/log|" "$FLINK_HOME/conf/config.yaml"
fi

# Download Kafka connector (skip if already exists)
if [ ! -f "$FLINK_HOME/lib/flink-sql-connector-kafka-3.4.0-1.20.jar" ]; then
    curl -L -o $FLINK_HOME/lib/flink-sql-connector-kafka-3.4.0-1.20.jar https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.4.0-1.20/flink-sql-connector-kafka-3.4.0-1.20.jar
else
    echo "flink-sql-connector-kafka-3.4.0-1.20.jar already exists, skipping download"
fi

# Create a virtual environment in a directory named 'venv'
if [ ! -d "venv" ]; then
    python3 -m venv venv
fi

# Activate the virtual environment
# On Linux/macOS:
source venv/bin/activate

pip install "flink-agents==0.2.1" "apache-flink==1.20.3" "elasticsearch~=8.19" "setuptools>=75.3,<82"

# Set PYTHONPATH to your Python site-packages directory
export PYTHONPATH=$(python -c 'import sysconfig; print(sysconfig.get_paths()["purelib"])')

# Copy the JAR from the Python package to Flink's lib directory
cp $PYTHONPATH/flink_agents/lib/common/flink-agents-dist-*.jar $FLINK_HOME/lib/
cp $PYTHONPATH/flink_agents/lib/flink-1.20/flink-agents-dist-*.jar $FLINK_HOME/lib/

# Set environment variable for metric history directory and job metadata directory
export METRIC_HISTORY_DIR="$PROJECT_DIR/tmp/metric_history"
export JOB_METADATA_DIR="$PROJECT_DIR/tmp/job_metadata"

# Start JobManager
echo "Starting JobManager..."
$FLINK_HOME/bin/jobmanager.sh start

# Start TaskManagers
echo "Starting $NUM_TASK_MANAGERS TaskManager(s)..."
for i in $(seq 1 $NUM_TASK_MANAGERS); do
    echo "  Starting TaskManager $i..."
    $FLINK_HOME/bin/taskmanager.sh start
done

echo "Flink cluster started successfully!"
echo "  Web UI: http://localhost:8082"

# Start watermark collector in background
echo "Starting watermark collector..."
"$SCRIPT_DIR/start_metric_collector.sh" --background
