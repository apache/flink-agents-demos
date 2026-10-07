#!/usr/bin/env bash
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
#
# Stateful operations agent demo. See docs/recording-runbook.md.
#
#   setup            one time: agent Flink, venv, flink-agents (main + #1161), target job jar, model
#   up               Kafka + topics, target cluster (NONE, skewed) + ClickstreamEnrichment,
#                    agent cluster (2 TaskManagers), Ollama warm-up, platform-sim (background)
#   platform         run platform-sim in the foreground instead (logs in the terminal)
#   agent            submit the operations agent job
#   fleet-incident   start the SIMULATED fleet incident (fleet/payments-sessionizer)
#   kill-agent-tm [--wait]
#                    kill -9 the agent TaskManager that sent the in-flight redeploy;
#                    with --wait, first wait for a redeploy to start (then 5s more)
#   reset            back to the starting point for another take
#   status           what is running
#   down [--all]     stop the clusters and platform-sim (--all: Kafka too, unload the model)
#
# Environment: TM_CPUS (target TaskManager CPUs, default 0.5: verified for recording, easy on a laptop),
# TARGET_RATE (target job input, records/s; default 19000 × TM_CPUS, i.e. 9500),
# WINDOW_SECONDS (default 15), COOLDOWN_SECONDS (default 30), KILL_DELAY (default 5).
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
FLINK_HOME="$PROJECT_DIR/flink-2.2.1"
TMP="$PROJECT_DIR/tmp"
PLATFORM="http://localhost:8090"
AGENT_REST="http://localhost:8082"
TARGET_REST="http://localhost:8081"
OLLAMA="http://localhost:11434"
MODEL="${OLLAMA_MODEL:-qwen3:8b}"
KAFKA_CONTAINER="stateful-ops-kafka"
export TM_CPUS="${TM_CPUS:-0.5}"
export WINDOW_SECONDS="${WINDOW_SECONDS:-15}"
export COOLDOWN_SECONDS="${COOLDOWN_SECONDS:-30}"
export OPS_RUNBOOK="$PROJECT_DIR/skills/balanced-task-scheduling/SKILL.md"

say() { printf '\033[1m==> %s\033[0m\n' "$*"; }
die() { printf '\033[31m%s\033[0m\n' "$*" >&2; exit 1; }
venv() {
    # shellcheck disable=SC1091
    source "$PROJECT_DIR/venv/bin/activate"
    PYTHONPATH="$(python -c 'import sysconfig; print(sysconfig.get_paths()["purelib"])')"
    export PYTHONPATH
}
py() { "$PROJECT_DIR/venv/bin/python" "$@"; }
target_ctl() { py "$PROJECT_DIR/target-cluster/target_ctl.py" "$@"; }
up_ok() { curl -sf -o /dev/null "$1"; }

kafka_topics() { docker exec "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 "$@"; }

start_kafka() {
    docker compose -f "$PROJECT_DIR/docker-compose.yml" up -d kafka >/dev/null 2>&1
    for _ in $(seq 1 30); do kafka_topics --list >/dev/null 2>&1 && break; sleep 2; done
    kafka_topics --create --if-not-exists --topic ops_metrics --partitions 1 >/dev/null
    kafka_topics --create --if-not-exists --topic ops_records --partitions 1 >/dev/null
    kafka_topics --create --if-not-exists --topic agent_action_state --partitions 8 >/dev/null
    say "Kafka up (ops_metrics, ops_records, agent_action_state)"
}

# Is ClickstreamEnrichment running in NONE mode with task skew >= 2? Waits up to 30s for metrics.
target_skewed() {
    PYTHONPATH="$PROJECT_DIR/target-cluster:$PROJECT_DIR/platform-sim" py - <<'EOF'
import sys, time
import collector
for _ in range(15):
    s = collector.sample("t", time.time())
    if s.get("running"):
        counts = list(s["tasks_per_tm"].values())
        print(f"   target: mode {s['config']['mode']}, tasks per TM {s['tasks_per_tm']}, cpu {s['cpu']}")
        sys.exit(0 if s["config"]["mode"] == "NONE" and max(counts) - min(counts) >= 2 else 1)
    time.sleep(2)
sys.exit(1)
EOF
}

# Target cluster in NONE mode with a skewed placement. NONE placement is not
# guaranteed to skew, so recreate (up to 3 times) until it does.
ensure_target() {
    local fresh="${1:-}"
    for attempt in 1 2 3; do
        if [ -n "$fresh" ] || ! target_skewed >/dev/null 2>&1; then
            say "Target cluster: NONE mode, TM_CPUS=$TM_CPUS, submit ClickstreamEnrichment (attempt $attempt)"
            target_ctl up --mode NONE --interval 20 >/dev/null
            target_ctl submit >/dev/null
        fi
        if target_skewed; then say "Target skewed: ready ($TARGET_REST)"; return 0; fi
        fresh=1
    done
    die "Target placement did not skew after 3 attempts; run: bin/demo.sh reset"
}

# A freshly started target needs ~30-60s (JIT warm-up) before the skewed placement makes it
# fall behind its input rate. Wait for that, so a take starts with backpressure on screen.
wait_target_backpressured() {
    say "Waiting for the target to fall behind its input (backpressure)..."
    PYTHONPATH="$PROJECT_DIR/target-cluster:$PROJECT_DIR/platform-sim" py - <<'EOF' || say "warning: target not backpressured yet; the first skewed window will come later"
import sys, time
import collector
streak = 0
for _ in range(60):
    s = collector.sample("t", time.time())
    bp = s.get("backpressure_ms") or 0
    streak = streak + 1 if s.get("running") and bp >= 300 else 0
    if streak >= 3:
        print(f"   target backpressured: Enrich {bp} ms/s")
        sys.exit(0)
    time.sleep(2)
sys.exit(1)
EOF
}

configure_agent_cluster() {
    local conf="$FLINK_HOME/conf/config.yaml"
    mkdir -p "$TMP/checkpoints" "$TMP/savepoints"
    cp "$PROJECT_DIR/bin/internal/flink_config.yaml" "$conf"
    sed -i '' -e "s|CHECKPOINT_DIR_PLACEHOLDER|$TMP/checkpoints|" -e "s|SAVEPOINT_DIR_PLACEHOLDER|$TMP/savepoints|" \
        -e "s|PYTHON_EXECUTABLE_PLACEHOLDER|$PROJECT_DIR/venv/bin/python|g" "$conf"
}

start_agent_cluster() {
    if up_ok "$AGENT_REST/overview"; then
        say "Agent cluster already up ($AGENT_REST)"
        return
    fi
    configure_agent_cluster
    "$PROJECT_DIR/bin/internal/agent_cluster.sh" start
}

restart_agent_cluster() {
    "$PROJECT_DIR/bin/internal/agent_cluster.sh" stop >/dev/null 2>&1 || true
    for _ in $(seq 1 15); do up_ok "$AGENT_REST/overview" || break; sleep 1; done
    configure_agent_cluster
    "$PROJECT_DIR/bin/internal/agent_cluster.sh" start
}

warm_model() {
    up_ok "$OLLAMA/api/tags" || die "Ollama is not running: open the Ollama app (or run: ollama serve)"
    curl -sf "$OLLAMA/api/generate" -d "{\"model\":\"$MODEL\",\"prompt\":\"ok\",\"stream\":false,\"think\":false,\"keep_alive\":\"60m\",\"options\":{\"num_predict\":1,\"num_ctx\":8192}}" >/dev/null \
        || die "Could not load $MODEL (ollama pull $MODEL)"
    say "Model $MODEL loaded (kept for 60m)"
}

start_platform() {
    if up_ok "$PLATFORM/api/state"; then
        say "platform-sim already up ($PLATFORM)"
        return
    fi
    mkdir -p "$TMP/platform"
    nohup "$PROJECT_DIR/venv/bin/python" "$PROJECT_DIR/platform-sim/platform_sim.py" >>"$TMP/platform/platform.log" 2>&1 &
    echo $! >"$TMP/platform/platform.pid"
    for _ in $(seq 1 20); do up_ok "$PLATFORM/api/state" && break; sleep 0.5; done
    up_ok "$PLATFORM/api/state" || die "platform-sim did not start; see $TMP/platform/platform.log"
    say "platform-sim up: dashboard $PLATFORM (log: tmp/platform/platform.log)"
}

stop_platform() {
    if [ -f "$TMP/platform/platform.pid" ]; then
        kill "$(cat "$TMP/platform/platform.pid")" 2>/dev/null || true
        rm -f "$TMP/platform/platform.pid"
    fi
}

running_agent_jobs() {
    curl -sf "$AGENT_REST/jobs/overview" 2>/dev/null | py -c '
import json, sys
for j in json.load(sys.stdin)["jobs"]:
    if j["state"] not in ("FINISHED", "CANCELED", "FAILED"):
        print(j["jid"])' || true
}

cancel_agent_jobs() {
    for jid in $(running_agent_jobs); do
        curl -sf -X PATCH "$AGENT_REST/jobs/$jid?mode=cancel" >/dev/null || true
        say "Cancelled agent job $jid"
    done
}

cmd_setup() {
    "$PROJECT_DIR/bin/internal/setup_flink.sh"
    if [ ! -f "$PROJECT_DIR/target-jobs/clickstream-enrichment/target/clickstream-enrichment.jar" ]; then
        (cd "$PROJECT_DIR/target-jobs/clickstream-enrichment" && mvn -q -B package)
    fi
    command -v ollama >/dev/null && ollama pull "$MODEL"
    say "Setup done. Next: bin/demo.sh up"
}

cmd_up() {
    start_kafka
    ensure_target
    start_agent_cluster
    warm_model
    start_platform
    wait_target_backpressured
    say "Ready. Dashboard $PLATFORM · target UI $TARGET_REST · agent UI $AGENT_REST. Next: bin/demo.sh agent"
}

cmd_agent() {
    [ -z "$(running_agent_jobs)" ] || die "An agent job is already running; run: bin/demo.sh reset"
    up_ok "$PLATFORM/api/state" || die "platform-sim is not running; run: bin/demo.sh up"
    venv
    say "Submitting the operations agent (windows ${WINDOW_SECONDS}s, cooldown ${COOLDOWN_SECONDS}s)"
    "$FLINK_HOME/bin/flink" run -d -Dpipeline.name=stateful-ops-agent -pym ops_job -pyfs "$PROJECT_DIR/agent-job"
    say "Agent job submitted: $AGENT_REST"
}

cmd_kill_agent_tm() {
    local pid="" job="" rid=""
    if [ "${1:-}" = "--wait" ]; then
        say "Waiting for the agent to start a redeploy..."
        until curl -sf "$PLATFORM/api/inflight" >/dev/null; do sleep 0.5; done
        sleep "${KILL_DELAY:-5}"
    fi
    read -r pid job rid < <(curl -sf "$PLATFORM/api/inflight" | py -c '
import json, sys
d = json.load(sys.stdin)
print(d.get("agent_pid") or "", d["job"], d["request_id"])') || die "No redeploy in flight"
    ps -p "$pid" -o command= 2>/dev/null | grep -q TaskManagerRunner || die "pid $pid is not a TaskManager"
    echo "Redeploy in flight: $rid ($job), sent by the agent TaskManager pid $pid"
    echo "\$ kill -9 $pid"
    kill -9 "$pid"
}

cmd_reset() {
    cancel_agent_jobs
    start_platform
    curl -sf -X POST "$PLATFORM/api/reset" >/dev/null && say "platform-sim reset"
    ensure_target fresh
    say "Restarting the agent cluster (fresh Python workers, 2 TaskManagers)"
    restart_agent_cluster
    warm_model
    wait_target_backpressured
    say "Ready for a take. Next: bin/demo.sh agent"
}

cmd_status() {
    up_ok "$PLATFORM/api/state" && echo "platform-sim: up ($PLATFORM)" || echo "platform-sim: down"
    docker ps --format '{{.Names}}: {{.Status}}' | grep -E "stateful-ops|target" || echo "no containers"
    if up_ok "$AGENT_REST/overview"; then
        curl -sf "$AGENT_REST/overview" | py -c 'import json,sys; o=json.load(sys.stdin); print("agent cluster: {taskmanagers} TMs, {slots-available}/{slots-total} slots free, jobs running {jobs-running}".format_map(o))'
    else
        echo "agent cluster: down"
    fi
    target_skewed 2>/dev/null || echo "   (target not skewed / not running)"
    curl -sf "$OLLAMA/api/ps" | py -c 'import json,sys; print("ollama loaded:", [m["name"] for m in json.load(sys.stdin)["models"]])' 2>/dev/null || echo "ollama: down"
}

cmd_down() {
    cancel_agent_jobs
    stop_platform
    "$PROJECT_DIR/bin/internal/agent_cluster.sh" stop >/dev/null 2>&1 || true
    target_ctl down >/dev/null 2>&1 || true
    if [ "${1:-}" = "--all" ]; then
        docker compose -f "$PROJECT_DIR/docker-compose.yml" down >/dev/null 2>&1 || true
        curl -sf "$OLLAMA/api/generate" -d "{\"model\":\"$MODEL\",\"keep_alive\":0}" >/dev/null 2>&1 || true
    fi
    say "Stopped"
}

case "${1:-}" in
    setup) cmd_setup ;;
    up) cmd_up ;;
    platform) stop_platform; mkdir -p "$TMP/platform"; exec "$PROJECT_DIR/venv/bin/python" "$PROJECT_DIR/platform-sim/platform_sim.py" ;;
    agent) cmd_agent ;;
    fleet-incident) curl -sf -X POST "$PLATFORM/api/fleet/incident" && echo ;;
    kill-agent-tm) shift; cmd_kill_agent_tm "${1:-}" ;;
    reset) cmd_reset ;;
    status) cmd_status ;;
    down) shift; cmd_down "${1:-}" ;;
    *) sed -n '20,37p' "$0"; exit 1 ;;
esac
