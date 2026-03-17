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

# Consume operations record from Kafka and save as markdown files
# Usage: ./consume_operations_record.sh [output_dir]

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN_DIR="$(dirname "$SCRIPT_DIR")"
PROJECT_ROOT="$(dirname "$BIN_DIR")"
OUTPUT_DIR="${1:-$PROJECT_ROOT/tmp/operations_record}"

cd "$PROJECT_ROOT"

source venv/bin/activate

# Ensure output directory exists and remove existing files
if [ -d "$OUTPUT_DIR" ]; then
  rm -rf "$OUTPUT_DIR"
fi
mkdir -p "$OUTPUT_DIR/normal"
mkdir -p "$OUTPUT_DIR/auto_remediated"
mkdir -p "$OUTPUT_DIR/manual_intervention"

echo "=========================================="
echo "Kafka Operations Record Consumer"
echo "Output directory: ${OUTPUT_DIR}"
echo "Press Ctrl+C to exit"
echo "=========================================="

docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic operations_record \
  --from-beginning \
  2>/dev/null | while read -r line; do

    # Skip empty lines
    if [ -z "$line" ]; then
        continue
    fi

    echo "Received Operations Record: ${line:0:100}..."

    # Use Python to parse data and generate markdown file
    export KAFKA_MESSAGE="$line"
    export OUTPUT_DIR_ENV="$OUTPUT_DIR"

    python3 -c '
import ast
import os

try:
    line = os.environ.get("KAFKA_MESSAGE", "")
    output_dir = os.environ.get("OUTPUT_DIR_ENV")

    # Use ast.literal_eval to parse Python dict format (single quotes)
    data = ast.literal_eval(line)

    job_id = data.get("job_id", "unknown")
    job_name = data.get("job_name", "unknown")
    base_url = data.get("base_url", "")
    status = data.get("status", "")
    start_time = data.get("start_time", "")
    end_time = data.get("end_time", "")
    duration = data.get("duration", "")
    diagnosis_result = data.get("diagnosis_result", "")
    need_intervention = data.get("need_intervention", False)
    remedy_process = data.get("remedy_process", "")


    # Determine subdirectory based on operational status
    if need_intervention:
        subdir = "manual_intervention"
    elif len(remedy_process) > 0:
        subdir = "auto_remediated"
    else:
        subdir = "normal"

    # Generate filename
    clean_job_name = job_name.replace(" ", "")
    filename = f"{output_dir}/{subdir}/{clean_job_name}-{end_time}.md"

    if len(remedy_process) > 0:
        # Build markdown content
        content = f"""---
job_id: {job_id}
base_url: {base_url}
status: {status}
need_intervention: {need_intervention}
start_time: {start_time}
end_time: {end_time}
duration: {duration}
---

{diagnosis_result}

### Remedy Process

{remedy_process}
"""
    else:
        content = f"""---
job_id: {job_id}
base_url: {base_url}
status: {status}
need_intervention: {need_intervention}
start_time: {start_time}
end_time: {end_time}
duration: {duration}
---

{diagnosis_result}
    """

    with open(filename, "w", encoding="utf-8") as f:
        f.write(content)

    print(f"Saved: {filename}")

except (ValueError, SyntaxError) as e:
    print(f"Data parsing error: {e}")
except Exception as e:
    print(f"Processing error: {e}")
'

done
