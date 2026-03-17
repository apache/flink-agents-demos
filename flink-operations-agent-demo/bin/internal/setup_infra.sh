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

echo "=========================================="
echo "  Starting Infrastructure Services"
echo "=========================================="

cd "$PROJECT_DIR"

echo "[1/2] Cleaning up old containers..."
docker rm -f elasticsearch kafka 2>/dev/null || true

echo "[2/2] Starting services with docker-compose..."
docker-compose up -d

echo "Waiting for services to be ready..."

echo "  - Waiting for Elasticsearch..."
for i in {1..30}; do
    if curl -s http://localhost:9200 >/dev/null 2>&1; then
        echo "  - Elasticsearch is ready!"
        break
    fi
    if [ $i -eq 30 ]; then
        echo "  - Warning: Elasticsearch may not be ready yet"
    fi
    sleep 2
done

echo "  - Waiting for Kafka..."
kafka_ready=false
for i in {1..30}; do
    if docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list >/dev/null 2>&1; then
        echo "  - Kafka is ready!"
        kafka_ready=true
        break
    fi
    sleep 2
done
if [ "$kafka_ready" = false ]; then
    echo "  - Warning: Kafka may not be ready yet"
fi

echo "  - Creating Kafka topics..."
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic job_info --partitions 1 --replication-factor 1
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic operations_record --partitions 1 --replication-factor 1

echo "  - Verifying topics creation..."
max_retries=10
retry_count=0
while [ $retry_count -lt $max_retries ]; do
  topics=$(docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list 2>/dev/null)
  if echo "$topics" | grep -q "job_info" && echo "$topics" | grep -q "operations_record"; then
    echo "  - Kafka topics created successfully: job_info, operations_record"
    break
  fi
  retry_count=$((retry_count + 1))
  if [ $retry_count -lt $max_retries ]; then
    echo "  - Waiting for topics to be ready... (attempt $retry_count/$max_retries)"
    sleep 2
  else
    echo "  - Warning: Topics verification timeout, but they may still be created"
  fi
done

echo ""
echo "=========================================="
echo "  Infrastructure Services Status"
echo "=========================================="
docker-compose ps

echo ""
echo "=========================================="
echo "  Service Endpoints"
echo "=========================================="
echo "  Kafka:         localhost:9092"
echo "  Elasticsearch: localhost:9200"
echo "=========================================="
