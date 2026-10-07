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
"""Observe and reconfigure the TARGET Flink cluster (stdlib only).

Used by the demo scripts and, later, by the agent's tools:
  status                       cluster config + per-TaskManager task layout and load
  submit [--savepoint PATH]    upload and run the ClickstreamEnrichment job
  redeploy --mode M [--interval MS]
                               stop-with-savepoint, recreate the cluster with the
                               new scheduling config, resubmit from the savepoint
  down                         stop the cluster
"""
import argparse
import json
import os
import subprocess
import sys
import time
import urllib.request
import uuid

HERE = os.path.dirname(os.path.abspath(__file__))
COMPOSE_FILE = os.path.join(HERE, "docker-compose.yml")
JOB_JAR = os.path.join(
    HERE, "..", "target-jobs", "clickstream-enrichment", "target", "clickstream-enrichment.jar"
)
REST = os.environ.get("TARGET_FLINK_REST", "http://localhost:8081")
JOB_NAME = "ClickstreamEnrichment"
NUM_TASKMANAGERS = 3
# Matches metrics.fetcher.update-interval in docker-compose.yml.
METRICS_FETCH_INTERVAL_S = 2.0
# Input rate for the whole job, records/s (0 = unbounded). It sits between the job's capacity
# with skewed placement (~7.5k/s at TM_CPUS=0.5) and with balanced placement (~12k/s), so the job
# falls behind and backpressures only while placement is skewed; once balanced it has headroom and
# the Flink UI stops showing it busy. Capacity scales with TM_CPUS.
JOB_RATE = int(os.environ.get("TARGET_RATE") or 19_000 * float(os.environ.get("TM_CPUS") or 1))


def _request(method, path, body=None, headers=None, timeout=30):
    data = json.dumps(body).encode() if isinstance(body, (dict, list)) else body
    hdrs = {"Content-Type": "application/json"} if isinstance(body, (dict, list)) else {}
    hdrs.update(headers or {})
    req = urllib.request.Request(REST + path, data=data, method=method, headers=hdrs)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        raw = resp.read()
        return json.loads(raw) if raw else {}


def _get(path):
    return _request("GET", path)


def cluster_config():
    keys = ("taskmanager.load-balance.mode", "slot.request.max-interval")
    entries = {e["key"]: e["value"] for e in _get("/jobmanager/config")}
    return {k: entries.get(k) for k in keys}


def running_job_id(name=JOB_NAME):
    for job in _get("/jobs/overview")["jobs"]:
        if job["name"] == name and job["state"] == "RUNNING":
            return job["jid"]
    return None


def _metric(path, name):
    values = _get(f"{path}?get={name}")
    return float(values[0]["value"]) if values else None


def layout(jid):
    """Per TaskManager: hosted subtasks with their busy/backpressured ms per second."""
    per_tm = {}
    for vertex in _get(f"/jobs/{jid}")["vertices"]:
        vid = vertex["id"]
        parts = vertex["name"].split(" -> ")
        vname = next((p for p in parts if not p.startswith("Source:")), parts[0])
        for st in _get(f"/jobs/{jid}/vertices/{vid}")["subtasks"]:
            base = f"/jobs/{jid}/vertices/{vid}/subtasks/{st['subtask']}/metrics"
            per_tm.setdefault(st["taskmanager-id"], []).append(
                {
                    "task": f"{vname}#{st['subtask']}",
                    "status": st["status"],
                    "busy_ms": _metric(base, "busyTimeMsPerSecond"),
                    "backpressured_ms": _metric(base, "backPressuredTimeMsPerSecond"),
                }
            )
    return dict(sorted(per_tm.items()))


def throughput(jid):
    """Records/s entering the last vertex (SessionScore), summed over subtasks.

    Flink's per-second meter averages over 60s, so it lags a redeploy by about a minute;
    use measure_throughput() for a fresh reading.
    """
    vertices = _get(f"/jobs/{jid}")["vertices"]
    last = vertices[-1]["id"]
    values = _get(
        f"/jobs/{jid}/vertices/{last}/subtasks/metrics?get=numRecordsInPerSecond&agg=sum"
    )
    return float(values[0]["sum"]) if values else None


def _records_in(jid, vid):
    values = _get(f"/jobs/{jid}/vertices/{vid}/subtasks/metrics?get=numRecordsIn&agg=sum")
    return float(values[0]["sum"]) if values else 0.0


def refresh_metrics():
    """Make the next metric reads current; returns when the fetched snapshot was taken.

    The JobManager's REST metric store refreshes asynchronously: a request triggers a
    fetch (at most every metrics.fetcher.update-interval) but is answered from the
    previous snapshot. A cold JobManager therefore answers its first query with nothing.
    """
    triggered = time.time()
    _get("/jobmanager/metrics?get=Status.JVM.CPU.Load")
    time.sleep(METRICS_FETCH_INTERVAL_S + 0.5)
    return triggered


def measure_throughput(jid, seconds=20):
    """Records/s into the last vertex over the next `seconds`, from counter deltas."""
    last = _get(f"/jobs/{jid}")["vertices"][-1]["id"]
    start_time = refresh_metrics()
    start_count = _records_in(jid, last)
    time.sleep(seconds)
    end_time = refresh_metrics()
    return (_records_in(jid, last) - start_count) / (end_time - start_time)


def tm_cpu():
    """JVM CPU load per TaskManager (container-aware: 1.0 = its one CPU is saturated)."""
    out = {}
    for tm in _get("/taskmanagers")["taskmanagers"]:
        out[tm["id"]] = _metric(f"/taskmanagers/{tm['id']}/metrics", "Status.JVM.CPU.Load")
    return dict(sorted(out.items()))


def status():
    jid = running_job_id()
    result = {"config": cluster_config(), "job_id": jid}
    if jid:
        refresh_metrics()
        tasks = layout(jid)
        cpu = tm_cpu()
        result["taskmanagers"] = {
            tm: {
                "num_tasks": len(ts),
                "cpu_load": cpu.get(tm),
                "tasks": [t["task"] for t in ts],
                "max_busy_ms": max((t["busy_ms"] or 0) for t in ts),
            }
            for tm, ts in tasks.items()
        }
        counts = [v["num_tasks"] for v in result["taskmanagers"].values()]
        result["task_skew"] = max(counts) - min(counts)
        result["records_per_sec"] = throughput(jid)
    return result


def _upload_jar(path):
    boundary = uuid.uuid4().hex
    with open(path, "rb") as f:
        content = f.read()
    body = (
        f"--{boundary}\r\nContent-Disposition: form-data; name=\"jarfile\"; "
        f"filename=\"{os.path.basename(path)}\"\r\nContent-Type: application/x-java-archive\r\n\r\n"
    ).encode() + content + f"\r\n--{boundary}--\r\n".encode()
    resp = _request(
        "POST",
        "/jars/upload",
        body,
        {"Content-Type": f"multipart/form-data; boundary={boundary}"},
        timeout=120,
    )
    return resp["filename"].split("/")[-1]


def submit(savepoint=None, program_args=None):
    jar_id = _upload_jar(JOB_JAR)
    args = list(program_args or [])
    if "--rate" not in args and JOB_RATE > 0:
        args += ["--rate", str(JOB_RATE)]
    body = {"programArgsList": args}
    if savepoint:
        body["savepointPath"] = savepoint
    return _request("POST", f"/jars/{jar_id}/run", body, timeout=120)["jobid"]


def stop_with_savepoint(jid, timeout_s=120):
    trigger = _request("POST", f"/jobs/{jid}/stop", {"drain": False})["request-id"]
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        resp = _get(f"/jobs/{jid}/savepoints/{trigger}")
        if resp["status"]["id"] == "COMPLETED":
            op = resp["operation"]
            if "failure-cause" in op:
                raise RuntimeError(op["failure-cause"].get("stack-trace", "savepoint failed")[:2000])
            return op["location"]
        time.sleep(1)
    raise TimeoutError(f"stop-with-savepoint of {jid} did not finish in {timeout_s}s")


def wait_for_cluster(timeout_s=120):
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            if len(_get("/taskmanagers")["taskmanagers"]) >= NUM_TASKMANAGERS:
                return
        except OSError:
            pass
        time.sleep(1)
    raise TimeoutError("target cluster did not come up")


def wait_for_running(jid, timeout_s=120):
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        vertices = _get(f"/jobs/{jid}")["vertices"]
        if all(v["status"] == "RUNNING" for v in vertices):
            return
        time.sleep(1)
    raise TimeoutError(f"job {jid} did not reach RUNNING")


def compose(*args, mode=None, interval_ms=None):
    env = dict(os.environ)
    if mode:
        env["LOAD_BALANCE_MODE"] = mode
    if interval_ms is not None:
        env["SLOT_REQUEST_MAX_INTERVAL"] = str(interval_ms)
    subprocess.run(
        ["docker", "compose", "-f", COMPOSE_FILE, *args],
        check=True,
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def up(mode="NONE", interval_ms=20):
    compose("up", "-d", "--force-recreate", mode=mode, interval_ms=interval_ms)
    wait_for_cluster()


def redeploy(mode, interval_ms=20, program_args=None):
    """Apply a scheduling config: savepoint, recreate the cluster, restore the job."""
    jid = running_job_id()
    savepoint = stop_with_savepoint(jid) if jid else None
    up(mode, interval_ms)
    new_jid = submit(savepoint, program_args)
    wait_for_running(new_jid)
    return {"savepoint": savepoint, "job_id": new_jid}


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("status")
    p_up = sub.add_parser("up")
    p_up.add_argument("--mode", default="NONE")
    p_up.add_argument("--interval", type=int, default=20)
    p_submit = sub.add_parser("submit")
    p_submit.add_argument("--savepoint")
    p_submit.add_argument("job_args", nargs=argparse.REMAINDER)
    p_redeploy = sub.add_parser("redeploy")
    p_redeploy.add_argument("--mode", required=True)
    p_redeploy.add_argument("--interval", type=int, default=20)
    p_redeploy.add_argument("job_args", nargs=argparse.REMAINDER)
    sub.add_parser("down")
    args = parser.parse_args()

    if args.cmd == "status":
        result = status()
    elif args.cmd == "up":
        up(args.mode, args.interval)
        result = cluster_config()
    elif args.cmd == "submit":
        jid = submit(args.savepoint, args.job_args)
        wait_for_running(jid)
        result = {"job_id": jid}
    elif args.cmd == "redeploy":
        result = redeploy(args.mode, args.interval, args.job_args)
    else:
        compose("down")
        result = {"down": True}
    json.dump(result, sys.stdout, indent=2)
    print()


if __name__ == "__main__":
    main()
