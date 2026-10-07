#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

# Start/stop the AGENT Flink cluster: 1 JobManager + NUM_TASK_MANAGERS TaskManagers.
# Usage: agent_cluster.sh start|add-tm|stop

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$(dirname "$SCRIPT_DIR")")"
FLINK_HOME="${FLINK_HOME:-$PROJECT_DIR/flink-2.2.1}"
NUM_TASK_MANAGERS="${NUM_TASK_MANAGERS:-2}"

case "$1" in
    start)
        source "$PROJECT_DIR/venv/bin/activate"
        # The embedded interpreter (Pemja) in the TaskManagers needs the venv packages.
        export PYTHONPATH=$(python -c 'import sysconfig; print(sysconfig.get_paths()["purelib"])')
        "$FLINK_HOME/bin/jobmanager.sh" start
        for _ in $(seq 1 "$NUM_TASK_MANAGERS"); do
            "$FLINK_HOME/bin/taskmanager.sh" start
        done
        for _ in $(seq 1 30); do
            if curl -sf http://localhost:8082/overview | grep -q "\"taskmanagers\":$NUM_TASK_MANAGERS"; then
                echo "Agent cluster ready: http://localhost:8082"
                exit 0
            fi
            sleep 2
        done
        echo "Agent cluster did not become ready in time" >&2
        exit 1
        ;;
    add-tm)
        # Bring a TaskManager back after a chaos kill.
        source "$PROJECT_DIR/venv/bin/activate"
        export PYTHONPATH=$(python -c 'import sysconfig; print(sysconfig.get_paths()["purelib"])')
        "$FLINK_HOME/bin/taskmanager.sh" start
        ;;
    stop)
        "$FLINK_HOME/bin/taskmanager.sh" stop-all
        "$FLINK_HOME/bin/jobmanager.sh" stop-all
        ;;
    *)
        echo "Usage: $0 start|add-tm|stop" >&2
        exit 1
        ;;
esac
