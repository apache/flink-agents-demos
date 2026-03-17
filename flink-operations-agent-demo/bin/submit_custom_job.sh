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

# Submit custom Flink job script
# Usage: ./submit_custom_job.sh -j <jar_path> -c <entry_class> [-p <parallelism>] [-n <job_name>]

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
METADATA_DIR="$PROJECT_DIR/tmp/job_metadata"
FLINK_REST_URL="http://localhost:8082"

# Create metadata directory if not exists
mkdir -p "$METADATA_DIR"

# Default values
PARALLELISM=1
JOB_NAME=""

# Parse command line arguments
while getopts "j:c:p:n:h" opt; do
    case $opt in
        j)
            JAR_PATH="$OPTARG"
            ;;
        c)
            ENTRY_CLASS="$OPTARG"
            ;;
        p)
            PARALLELISM="$OPTARG"
            ;;
        n)
            JOB_NAME="$OPTARG"
            ;;
        h)
            echo "Usage: $0 -j <jar_path> -c <entry_class> [-p <parallelism>] [-n <job_name>]"
            echo ""
            echo "Options:"
            echo "  -j <jar_path>      Path to the Flink job JAR file (required)"
            echo "  -c <entry_class>   Fully qualified main class name (required)"
            echo "  -p <parallelism>   Job parallelism (default: 1)"
            echo "  -n <job_name>      Custom job name (optional)"
            echo "  -h                 Show this help message"
            echo ""
            echo "Example:"
            echo "  $0 -j /path/to/job.jar -c com.example.MyJob"
            echo "  $0 -j /path/to/job.jar -c com.example.MyJob -p 4 -n MyCustomJob"
            exit 0
            ;;
        \?)
            echo "Error: Invalid option -$OPTARG" >&2
            echo "Use -h for help"
            exit 1
            ;;
        :)
            echo "Error: Option -$OPTARG requires an argument." >&2
            exit 1
            ;;
    esac
done

# Validate required arguments
if [ -z "$JAR_PATH" ]; then
    echo "Error: JAR path is required"
    echo "Use -h for help"
    exit 1
fi

if [ -z "$ENTRY_CLASS" ]; then
    echo "Error: Entry class is required"
    echo "Use -h for help"
    exit 1
fi

# Check if JAR file exists
if [ ! -f "$JAR_PATH" ]; then
    echo "Error: JAR file does not exist: $JAR_PATH"
    exit 1
fi

# Set default job name if not provided
if [ -z "$JOB_NAME" ]; then
    JOB_NAME="${ENTRY_CLASS##*.}"
fi

echo "=========================================="
echo "Submitting custom Flink job"
echo "JAR path: $JAR_PATH"
echo "Entry class: $ENTRY_CLASS"
echo "Parallelism: $PARALLELISM"
echo "Job name: $JOB_NAME"
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

echo "Submitting job: $JOB_NAME"
SUBMIT_RESPONSE=$(curl -s -X POST "$FLINK_REST_URL/jars/$JAR_ID/run" \
    -H "Content-Type: application/json" \
    -d "{\"entryClass\":\"$ENTRY_CLASS\",\"parallelism\":$PARALLELISM,\"programArgs\":\"--jobName $JOB_NAME\"}")

JOB_ID=$(echo "$SUBMIT_RESPONSE" | grep -oE '"jobid":"[a-f0-9]{32}"' | sed 's/"jobid":"//;s/"//')

if [ -z "$JOB_ID" ]; then
    echo "Error: Failed to submit job"
    echo "Response: $SUBMIT_RESPONSE"
    exit 1
fi

echo "Job ID: $JOB_ID"

# Save job metadata to individual file
METADATA_FILE="$METADATA_DIR/${JOB_ID}.json"
cat > "$METADATA_FILE" << EOF
{
  "job_id": "$JOB_ID",
  "jar_id": "$JAR_ID",
  "entry_class": "$ENTRY_CLASS",
  "job_name": "$JOB_NAME",
  "parallelism": $PARALLELISM,
  "submit_time": "$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
}
EOF
echo "Metadata saved to: $METADATA_FILE"
echo ""

echo "=========================================="
echo "Job submitted successfully"
echo "Job ID: $JOB_ID"
echo "=========================================="
