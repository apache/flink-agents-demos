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

# Batch submit 4 Flink jobs script
# Usage: ./submit_sample_jobs.sh

# Flink job JAR path (relative to script directory)
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BIN_DIR="$(dirname "$SCRIPT_DIR")"
PROJECT_ROOT="$(dirname "$BIN_DIR")"
PROJECT_DIR="$PROJECT_ROOT/sample-jobs"
JAR_PATH="$PROJECT_DIR/target/sample-jobs-1.0.0.jar"
FLINK_REST_URL="http://localhost:8082"
METADATA_DIR="$PROJECT_ROOT/tmp/job_metadata"

# Create metadata directory if not exists
mkdir -p "$METADATA_DIR"

# Build the project first
echo "Building sample-jobs project..."
cd "$PROJECT_DIR" && mvn clean install -DskipTests
if [ $? -ne 0 ]; then
    echo "Error: Maven build failed"
    exit 1
fi
cd "$SCRIPT_DIR"

# 4 job main classes
JOB_CLASSES=(
    "org.apache.flink.sample.jobs.NormalJob"
    "org.apache.flink.sample.jobs.BackPressuredJob"
    "org.apache.flink.sample.jobs.BackPressuredCatchingUpJob"
    "org.apache.flink.sample.jobs.FailedStartupJob"
)

# Check if JAR file exists
if [ ! -f "$JAR_PATH" ]; then
    echo "Error: JAR file does not exist: $JAR_PATH"
    echo "Please build the project first: cd sample-jobs && mvn clean package"
    exit 1
fi

echo "=========================================="
echo "Submitting 4 Flink jobs"
echo "JAR path: $JAR_PATH"
echo "=========================================="
echo ""

echo "Uploading JAR to Flink cluster..."
UPLOAD_RESPONSE=$(curl -s -X POST -F "jarfile=@$JAR_PATH" "$FLINK_REST_URL/jars/upload")

if echo "$UPLOAD_RESPONSE" | grep -q '"errors"'; then
    echo "Error: Failed to upload JAR"
    echo "Response: $UPLOAD_RESPONSE"
    exit 1
fi

# Extract filename and use basename as JAR ID
JAR_FULL_PATH=$(echo "$UPLOAD_RESPONSE" | grep -oE '"filename":"[^"]+"' | sed 's/"filename":"//;s/"//')
JAR_ID=$(basename "$JAR_FULL_PATH")

if [ -z "$JAR_ID" ]; then
    echo "Error: Failed to extract JAR ID"
    echo "Response: $UPLOAD_RESPONSE"
    exit 1
fi

echo "JAR uploaded successfully"
echo "Full path: $JAR_FULL_PATH"
echo "JAR ID: $JAR_ID"
echo ""

# Submit jobs and collect results
declare -a JOB_NAMES
declare -a JOB_IDS

for CLASS in "${JOB_CLASSES[@]}"; do
    JOB_NAME="${CLASS##*.}"

    echo "Submitting job: $JOB_NAME"
    
    SUBMIT_RESPONSE=$(curl -s -X POST "$FLINK_REST_URL/jars/$JAR_ID/run" \
        -H "Content-Type: application/json" \
        -d "{\"entryClass\":\"$CLASS\",\"parallelism\":1}")

    JOB_ID=$(echo "$SUBMIT_RESPONSE" | grep -oE '"jobid":"[a-f0-9]{32}"' | sed 's/"jobid":"//;s/"//')

    if [ -z "$JOB_ID" ]; then
        JOB_ID="-"
        echo "Warning: Failed to extract Job ID for $JOB_NAME"
        echo "Response: $SUBMIT_RESPONSE"
    else
        echo "Job ID: $JOB_ID"
        
        # Save job metadata to individual file
        METADATA_FILE="$METADATA_DIR/${JOB_ID}.json"
        cat > "$METADATA_FILE" << EOF
{
  "job_id": "$JOB_ID",
  "jar_id": "$JAR_ID",
  "entry_class": "$CLASS",
  "job_name": "$JOB_NAME",
  "parallelism": 1,
  "submit_time": "$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
}
EOF
        echo "Metadata saved to: $METADATA_FILE"
    fi

    JOB_NAMES+=("$JOB_NAME")
    JOB_IDS+=("$JOB_ID")
    echo ""
done

# Print aligned results
echo "Submitted Jobs:"
echo "----------------------------------------"
printf "%-30s %s\n" "Job Name" "Job ID"
echo "----------------------------------------"
for i in "${!JOB_NAMES[@]}"; do
    printf "%-30s %s\n" "${JOB_NAMES[$i]}" "${JOB_IDS[$i]}"
done
echo "----------------------------------------"
echo ""
echo "All jobs submitted."