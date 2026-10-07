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

# Sets up the AGENT Flink cluster (Flink 2.2.1 + Flink Agents 0.3.1) and the
# Python virtual environment. Does not start anything.

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$(dirname "$SCRIPT_DIR")")"

FLINK_VERSION=2.2.1
KAFKA_CONNECTOR_VERSION=5.0.0-2.2
# Durable execution across a TaskManager crash needs apache/flink-agents#1161
# (not released yet), so flink-agents is built from source: main + that PR.
FLINK_AGENTS_REPO="${FLINK_AGENTS_REPO:-https://github.com/apache/flink-agents.git}"
FLINK_AGENTS_REF="${FLINK_AGENTS_REF:-pull/1161/head}"
FLINK_AGENTS_SRC="$PROJECT_DIR/third_party/flink-agents"

cd "$PROJECT_DIR"

if [ ! -d "flink-$FLINK_VERSION" ]; then
    if [ ! -f "flink-$FLINK_VERSION-bin-scala_2.12.tgz" ]; then
        curl -LO "https://archive.apache.org/dist/flink/flink-$FLINK_VERSION/flink-$FLINK_VERSION-bin-scala_2.12.tgz"
    fi
    tar -xzf "flink-$FLINK_VERSION-bin-scala_2.12.tgz"
fi
export FLINK_HOME="$PROJECT_DIR/flink-$FLINK_VERSION"

cp "$FLINK_HOME/opt/flink-python-$FLINK_VERSION.jar" "$FLINK_HOME/lib/"

KAFKA_JAR="flink-sql-connector-kafka-$KAFKA_CONNECTOR_VERSION.jar"
if [ ! -f "$FLINK_HOME/lib/$KAFKA_JAR" ]; then
    curl -L -o "$FLINK_HOME/lib/$KAFKA_JAR" \
        "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/$KAFKA_CONNECTOR_VERSION/$KAFKA_JAR"
fi

mkdir -p "$PROJECT_DIR/tmp/checkpoints" "$PROJECT_DIR/tmp/savepoints"
cp "$SCRIPT_DIR/flink_config.yaml" "$FLINK_HOME/conf/config.yaml"
if [[ "$OSTYPE" == "darwin"* ]]; then SED_I=(sed -i ''); else SED_I=(sed -i); fi
"${SED_I[@]}" "s|CHECKPOINT_DIR_PLACEHOLDER|$PROJECT_DIR/tmp/checkpoints|" "$FLINK_HOME/conf/config.yaml"
"${SED_I[@]}" "s|SAVEPOINT_DIR_PLACEHOLDER|$PROJECT_DIR/tmp/savepoints|" "$FLINK_HOME/conf/config.yaml"
"${SED_I[@]}" "s|PYTHON_EXECUTABLE_PLACEHOLDER|$PROJECT_DIR/venv/bin/python|g" "$FLINK_HOME/conf/config.yaml"

if [ ! -d "venv" ]; then
    PYTHON_BIN=""
    for candidate in python3.11 python3.12 python3.10 python3; do
        if command -v "$candidate" >/dev/null 2>&1 && \
           "$candidate" -c 'import sys; sys.exit(0 if (3,10) <= sys.version_info[:2] <= (3,12) else 1)'; then
            PYTHON_BIN="$candidate"
            break
        fi
    done
    if [ -z "$PYTHON_BIN" ]; then
        echo "Error: Python 3.10-3.12 is required"
        exit 1
    fi
    "$PYTHON_BIN" -m venv venv
fi
source venv/bin/activate
BUILD_CONSTRAINT_FILE="$(mktemp)"
echo "setuptools<81" > "$BUILD_CONSTRAINT_FILE"
PIP_CONSTRAINT="$BUILD_CONSTRAINT_FILE" pip install -q \
    "apache-flink==$FLINK_VERSION" "kafka-python>=2.0" "setuptools>=75.3,<82"
rm -f "$BUILD_CONSTRAINT_FILE"

if [ ! -d "$FLINK_AGENTS_SRC" ]; then
    git clone -q "$FLINK_AGENTS_REPO" "$FLINK_AGENTS_SRC"
    git -C "$FLINK_AGENTS_SRC" fetch -q origin "$FLINK_AGENTS_REF:demo-build"
    git -C "$FLINK_AGENTS_SRC" checkout -q demo-build
fi
if [ "${REBUILD_FLINK_AGENTS:-0}" = "1" ] || \
   ! ls "$FLINK_AGENTS_SRC"/dist/flink-2.2/target/flink-agents-dist-flink-2.2-*-thin.jar >/dev/null 2>&1; then
    echo "Building flink-agents from $FLINK_AGENTS_SRC (JDK 17, a few minutes)..."
    # `clean` matters: an incremental build can re-shade a stale runtime jar.
    (cd "$FLINK_AGENTS_SRC" && JAVA_HOME="${JAVA17_HOME:-$(/usr/libexec/java_home -v 17)}" \
        mvn clean install -B -q -DskipTests -Dspotless.skip=true -Drat.skip=true \
        -pl dist/common,dist/flink-2.2 -am)
fi
JAR_DIR="$FLINK_AGENTS_SRC/python/flink_agents/lib"
rm -rf "$JAR_DIR/common" "$JAR_DIR/flink-2.2"
mkdir -p "$JAR_DIR/common" "$JAR_DIR/flink-2.2"
cp "$FLINK_AGENTS_SRC"/dist/common/target/flink-agents-dist-common-*-SNAPSHOT.jar "$JAR_DIR/common/"
cp "$FLINK_AGENTS_SRC"/dist/flink-2.2/target/flink-agents-dist-flink-2.2-*-thin.jar "$JAR_DIR/flink-2.2/"
export FLINK_AGENTS_SKIP_JAR_DOWNLOAD=1
pip install -q "$FLINK_AGENTS_SRC/python"
pip install -q --force-reinstall --no-deps "$FLINK_AGENTS_SRC/python"
unset FLINK_AGENTS_SKIP_JAR_DOWNLOAD

PURELIB=$(python -c 'import sysconfig; print(sysconfig.get_paths()["purelib"])')
# Python jobs: AgentsExecutionEnvironment registers the Flink Agents dist jars via
# pipeline.jars. Do NOT also copy them into $FLINK_HOME/lib, otherwise
# kafka-clients is loaded twice and the Kafka action state store fails with a
# ClassCastException (JmxReporter).
rm -f "$FLINK_HOME"/lib/flink-agents-dist-*.jar
echo "Flink Agents dist jars are provided by $PURELIB/flink_agents/lib (pipeline.jars)"

echo "Agent Flink cluster set up at $FLINK_HOME"
