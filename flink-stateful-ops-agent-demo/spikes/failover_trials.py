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
"""P0 spike: does a TaskManager failover under TASKS mode leave the job unbalanced?

Kills one TaskManager container per trial (rotating), restarts it after
RESTART_DELAY_S, waits for the job to recover, and prints the resulting layout.

Usage: python spikes/failover_trials.py <trials>
"""
import json
import os
import subprocess
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "target-cluster"))
import target_ctl as t  # noqa: E402

RESTART_DELAY_S = float(os.environ.get("RESTART_DELAY_S", "5"))
SETTLE_S = float(os.environ.get("SETTLE_S", "20"))


def job_state(jid):
    return t._get(f"/jobs/{jid}")["state"]


def num_restarts(jid):
    return int(t._metric(f"/jobs/{jid}/metrics", "numRestarts") or 0)


def trial(i):
    jid = t.running_job_id()
    victim = f"target-tm-{i % t.NUM_TASKMANAGERS + 1}"
    restarts_before = num_restarts(jid)
    killed_at = time.time()
    subprocess.run(["docker", "kill", victim], check=True, stdout=subprocess.DEVNULL)
    time.sleep(RESTART_DELAY_S)
    subprocess.run(["docker", "start", victim], check=True, stdout=subprocess.DEVNULL)
    deadline = time.time() + 300
    while time.time() < deadline:
        if num_restarts(jid) > restarts_before:
            try:
                t.wait_for_running(jid, timeout_s=5)
                break
            except TimeoutError:
                pass
        time.sleep(1)
    recovered_s = round(time.time() - killed_at, 1)
    time.sleep(SETTLE_S)
    t.refresh_metrics()
    tasks = t.layout(jid)
    counts = {tm: len(ts) for tm, ts in tasks.items()}
    return {
        "trial": i,
        "victim": victim,
        "config": t.cluster_config(),
        "state": job_state(jid),
        "restarts": num_restarts(jid) - restarts_before,
        "recovered_s": recovered_s,
        "tasks_per_tm": counts,
        "skew": max(counts.values()) - min(counts.values()),
        "tasks": {tm: [x["task"] for x in ts] for tm, ts in tasks.items()},
    }


if __name__ == "__main__":
    for i in range(int(sys.argv[1]) if len(sys.argv) > 1 else 3):
        print(json.dumps(trial(i)), flush=True)
