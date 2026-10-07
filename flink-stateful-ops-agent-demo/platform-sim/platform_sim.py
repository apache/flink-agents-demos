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
"""The platform around the agent, in one process (stands in for metrics, a
deployment API and a ticket system):

- collector: every 3s, one metric sample per watched job -> Kafka `ops_metrics`
  (the REAL target job from its REST API, plus the SIMULATED fleet)
- deployment API: executes every redeploy request it receives, without
  de-duplication, and logs each execution to tmp/platform/deployments.jsonl
- escalation API: files the agent's reports to tmp/platform/escalations/
- dashboard: http://localhost:8090, built from Kafka `ops_records` (the agent's
  output) and the platform's own view
"""
import json
import os
import re
import sys
import threading
import time
import traceback
import urllib.parse
import urllib.request
from collections import Counter, deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
sys.path.insert(0, str(ROOT / "target-cluster"))

import collector  # noqa: E402
import target_ctl  # noqa: E402
from fleet import INCIDENT_JOB, Fleet  # noqa: E402

KAFKA = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
PORT = int(os.environ.get("PLATFORM_PORT", "8090"))
SAMPLE_INTERVAL_S = 3
HERO = target_ctl.JOB_NAME
DATA = ROOT / "tmp" / "platform"
AGENT_REST = os.environ.get("AGENT_FLINK_REST", "http://localhost:8082")


def agent_job_status():
    """State of the latest agent job on the agent cluster, for the dashboard."""
    try:
        with urllib.request.urlopen(AGENT_REST + "/jobs/overview", timeout=2) as resp:
            jobs = json.loads(resp.read())["jobs"]
    except (OSError, ValueError, KeyError):
        return {"state": "UNREACHABLE"}
    if not jobs:
        return {"state": "NO JOB"}
    job = max(jobs, key=lambda j: j["start-time"])
    out = {"state": job["state"], "jid": job["jid"], "name": job["name"], "start_time": job["start-time"] / 1000}
    try:
        url = f"{AGENT_REST}/jobs/{job['jid']}/metrics?get=numRestarts"
        with urllib.request.urlopen(url, timeout=2) as resp:
            values = json.loads(resp.read())
        out["restarts"] = int(float(values[0]["value"])) if values else 0
    except (OSError, ValueError, KeyError, IndexError):
        pass
    return out


def log(msg):
    print(time.strftime("%H:%M:%S"), msg, flush=True)


def _safe(name):
    return re.sub(r"[^A-Za-z0-9._-]", "_", name)


class Platform:
    def __init__(self):
        self.lock = threading.RLock()
        self.real_lock = threading.Lock()
        self.fleet = Fleet()
        (DATA / "escalations").mkdir(parents=True, exist_ok=True)
        self.state_file = DATA / "state.json"
        saved = json.loads(self.state_file.read_text()) if self.state_file.exists() else {}
        self.ignore_before = saved.get("ignore_before", 0)
        self._clear()

    def _clear(self):
        self.samples = 0
        self.hero_samples = deque(maxlen=6)
        self.deployments = {}
        self.executions = []
        self.escalations = {}
        self.records = {}
        self.order = []
        self.duplicate_records = 0
        self.agent = {"state": "UNKNOWN"}

    def _ledger(self, event, rec):
        with open(DATA / "deployments.jsonl", "a") as f:
            f.write(json.dumps({"event": event, "at": time.time(), **rec}) + "\n")

    # ---- collector -------------------------------------------------------
    def collect_loop(self):
        producer = None
        next_t = time.time()
        seq = 0
        while True:
            try:
                if producer is None:
                    producer = KafkaProducer(
                        bootstrap_servers=KAFKA, linger_ms=20,
                        key_serializer=str.encode, value_serializer=lambda v: json.dumps(v).encode())
                ts = time.time()
                batch = [collector.sample(HERO, ts)] + self.fleet.samples(ts)
                for s in batch:
                    s.update(ts=ts, seq=seq)
                    producer.send("ops_metrics", key=s["job"], value=s, timestamp_ms=int(ts * 1000))
                producer.flush()
                seq += 1
                agent = agent_job_status()
                with self.lock:
                    self.samples += len(batch)
                    self.hero_samples.append(batch[0])
                    self.agent = agent
            except KafkaError as e:
                log(f"collector: Kafka error ({e!r}), reconnecting")
                producer = None
            except Exception:  # keep sampling whatever happens
                log("collector error: " + traceback.format_exc(limit=2))
            next_t = max(next_t + SAMPLE_INTERVAL_S, time.time() - SAMPLE_INTERVAL_S)
            time.sleep(max(0.0, next_t - time.time()))

    # ---- the agent's output ----------------------------------------------
    def records_loop(self):
        while True:
            try:
                consumer = KafkaConsumer(
                    "ops_records", bootstrap_servers=KAFKA, group_id=None, auto_offset_reset="earliest",
                    enable_auto_commit=False, value_deserializer=lambda b: json.loads(b))
                for msg in consumer:
                    rec = msg.value
                    with self.lock:
                        if rec.get("at", 0) < self.ignore_before:
                            continue
                        if rec["id"] in self.records:
                            self.duplicate_records += 1
                            continue
                        self.records[rec["id"]] = rec
                        self.order.append(rec["id"])
            except Exception:
                log("records consumer error: " + traceback.format_exc(limit=2))
                time.sleep(3)

    # ---- deployment API ----------------------------------------------------
    def create_deployment(self, body):
        rid = body["request_id"]
        with self.lock:
            n = sum(1 for e in self.executions if e["request_id"] == rid) + 1
            rec = {
                "request_id": rid, "job": body["job"], "target": {"mode": body["mode"],
                                                                   "interval_ms": int(body["interval_ms"])},
                "reason": body.get("reason"), "agent_pid": body.get("agent_pid"), "status": "running",
                "created_at": time.time(), "execution": n, "steps": [], "notes": [],
            }
            if n > 1:
                rec["notes"].append(f"DUPLICATE: request {rid} executed {n} times")
            self.deployments[rid] = rec
            self.executions.append({"request_id": rid, "job": body["job"], "at": rec["created_at"]})
        log(f"deploy {rid}: {rec['target']} (execution {n}, agent pid {rec['agent_pid']})")
        self._ledger("received", {k: rec[k] for k in ("request_id", "job", "target", "agent_pid", "execution")})
        threading.Thread(target=self._run_deployment, args=(rec,), daemon=True).start()
        return rec

    def _step(self, rec, name):
        with self.lock:
            if rec["steps"]:
                rec["steps"][-1]["done"] = True
            rec["steps"].append({"name": name, "at": time.time(), "done": False})

    def _run_deployment(self, rec):
        t0 = time.time()
        try:
            if rec["job"] == HERO:
                result = self._real_redeploy(rec)
            else:
                result = self.fleet.redeploy(rec["job"], rec["target"]["mode"], rec["target"]["interval_ms"],
                                             lambda name: self._step(rec, name))
            with self.lock:
                rec["steps"][-1]["done"] = True
                rec.update(status="done", result=result, took_s=round(time.time() - t0, 1))
        except Exception as e:
            with self.lock:
                rec.update(status="failed", error=f"{type(e).__name__}: {e}"[:500])
        log(f"deploy {rec['request_id']}: {rec['status']} in {round(time.time() - t0, 1)}s")
        self._ledger(rec["status"], {k: rec.get(k) for k in ("request_id", "job", "took_s", "result", "error")})

    def _real_redeploy(self, rec):
        """Savepoint, recreate the target cluster with the new config, restore."""
        with self.real_lock:
            self._step(rec, "stop-with-savepoint")
            jid = target_ctl.running_job_id()
            savepoint = target_ctl.stop_with_savepoint(jid) if jid else None
            self._step(rec, "recreate cluster")
            target_ctl.up(rec["target"]["mode"], rec["target"]["interval_ms"])
            self._step(rec, "submit from savepoint")
            new_jid = target_ctl.submit(savepoint)
            self._step(rec, "wait for RUNNING")
            target_ctl.wait_for_running(new_jid)
            return {"savepoint": savepoint, "job_id": new_jid}

    def reconciled(self, rid, body):
        with self.lock:
            rec = self.deployments.get(rid)
            if rec is None:
                return None
            rec["reconciled"] = {"at": time.time(), "agent_pid": body.get("agent_pid")}
            rec["notes"].append("agent recovered: its reconciler found this request and waited "
                                "for it instead of sending it again")
        log(f"deploy {rid}: reconciled by agent pid {body.get('agent_pid')}")
        self._ledger("reconciled", {"request_id": rid, "agent_pid": body.get("agent_pid")})
        return rec

    def inflight(self):
        with self.lock:
            running = [d for d in self.deployments.values() if d["status"] == "running"]
            return running[-1] if running else None

    # ---- escalation API ----------------------------------------------------
    def create_escalation(self, body):
        rid = body["request_id"]
        with self.lock:
            esc = self.escalations.get(rid) or {**body, "at": time.time(), "count": 0}
            esc["count"] += 1
            self.escalations[rid] = esc
        (DATA / "escalations" / f"{_safe(rid)}.md").write_text(f"# {body['title']}\n\n{body['body']}\n")
        log(f"escalation {rid} filed (count {esc['count']})")
        return esc

    # ---- control -------------------------------------------------------------
    def reset(self):
        with self.lock:
            self._clear()
            self.ignore_before = time.time()
            self.state_file.write_text(json.dumps({"ignore_before": self.ignore_before}))
        self.fleet.reset()
        log("reset")

    # ---- dashboard state -------------------------------------------------------
    def _live_rate(self):
        samples = [s for s in self.hero_samples if s.get("running")]
        if not samples:
            return None
        same = [s for s in samples if s["job_id"] == samples[-1]["job_id"]]
        deltas = sorted((b["records_in"] - a["records_in"]) / (b["ts"] - a["ts"])
                        for a, b in zip(same, same[1:]) if b["ts"] - a["ts"] >= 1)
        return round(deltas[len(deltas) // 2]) if deltas else None

    def _job_view(self, job, recs):
        decisions = [r for r in recs if r["kind"] == "decision"]
        deployments = [d for d in self.deployments.values() if d["job"] == job]
        escalations = [e for e in self.escalations.values() if e["job"] == job]
        return {
            "memory": recs[-1]["memory"] if recs else None,
            "window": recs[-1]["window"] if recs else None,
            "timeline": [_timeline_item(r) for r in recs[-12:]],
            "latest_decision": {k: decisions[-1].get(k) for k in
                                ("at", "decision", "source", "note", "allowed", "llm_seconds", "request_id")}
            if decisions else None,
            "redeploys": sum(1 for e in self.executions if e["job"] == job),
            "deployment": deployments[-1] if deployments else None,
            "escalation": escalations[-1] if escalations else None,
        }

    def state(self):
        with self.lock:
            recs = [self.records[i] for i in self.order]
            by_job = {}
            for r in recs:
                by_job.setdefault(r["job"], []).append(r)
            kinds = Counter(r["kind"] for r in recs)
            outcomes = Counter()
            for rs in by_job.values():
                for h in (rs[-1].get("memory") or {}).get("history") or []:
                    outcomes[h["outcome"]] += 1
            hero_live = self.hero_samples[-1] if self.hero_samples else None
            return {
                "now": time.time(),
                "counters": {
                    "samples": self.samples,
                    "windows": kinds["observe"] + kinds["decision"],
                    "llm_calls": sum(1 for r in recs if r["kind"] == "decision" and "llm_seconds" in r),
                    "side_effects": len(self.executions) + sum(e["count"] for e in self.escalations.values()),
                    "duplicate_records": self.duplicate_records,
                },
                "outcomes": {k: outcomes.get(k, 0) for k in ("KEPT", "REVERTED", "ESCALATED")},
                "agent": self.agent,
                "hero": {"job": HERO, "live": hero_live, "live_rate": self._live_rate(),
                         **self._job_view(HERO, by_job.get(HERO, []))},
                "fleet": {"jobs": self.fleet.summary(), "focus_job": INCIDENT_JOB,
                          "focus": self._job_view(INCIDENT_JOB, by_job.get(INCIDENT_JOB, []))},
            }


def _timeline_item(r):
    w = r.get("window") or {}
    if r["kind"] == "observe":
        text = r.get("note") or ""
    elif r["kind"] == "decision":
        d = r.get("decision") or {}
        text = f"{d.get('action')} · {d.get('runbook_step')}"
    elif r["kind"] == "applied":
        rc = r.get("receipt") or {}
        text = f"redeployed {r.get('request_id')} in {rc.get('took_s')}s" + (" (reconciled)" if rc.get("reconciled") else "")
    else:
        text = f"escalated {r.get('request_id')}"
    return {"at": r.get("at"), "window_end": w.get("window_end"), "kind": r["kind"], "text": text,
            "source": r.get("source"), "skew": w.get("task_skew"), "rate": w.get("records_per_sec"),
            "tasks_per_tm": w.get("tasks_per_tm")}


class Handler(BaseHTTPRequestHandler):
    platform: Platform = None

    def log_message(self, *args):
        pass

    def _send(self, code, obj=None, content_type="application/json", raw=None):
        body = raw if raw is not None else json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _parts(self):
        path = urllib.parse.urlparse(self.path).path
        return [urllib.parse.unquote(p) for p in path.strip("/").split("/") if p]

    def _body(self):
        n = int(self.headers.get("Content-Length") or 0)
        return json.loads(self.rfile.read(n) or b"{}")

    def do_GET(self):
        p, plat = self._parts(), self.platform
        if p in ([], ["index.html"]):
            return self._send(200, content_type="text/html; charset=utf-8",
                              raw=(HERE / "dashboard.html").read_bytes())
        if p == ["api", "state"]:
            return self._send(200, plat.state())
        if p == ["api", "inflight"]:
            rec = plat.inflight()
            return self._send(200 if rec else 404, rec or {"error": "no deployment in flight"})
        if p == ["api", "deployments"]:
            with plat.lock:
                return self._send(200, list(plat.deployments.values()))
        if len(p) == 3 and p[:2] == ["api", "deployments"]:
            with plat.lock:
                rec = plat.deployments.get(p[2])
                return self._send(200 if rec else 404, rec or {"error": "not found"})
        if len(p) == 3 and p[:2] == ["api", "escalations"]:
            with plat.lock:
                esc = plat.escalations.get(p[2])
                return self._send(200 if esc else 404, esc or {"error": "not found"})
        self._send(404, {"error": "not found"})

    def do_POST(self):
        p, plat = self._parts(), self.platform
        try:
            if p == ["api", "deployments"]:
                return self._send(202, plat.create_deployment(self._body()))
            if len(p) == 4 and p[:2] == ["api", "deployments"] and p[3] == "reconciled":
                rec = plat.reconciled(p[2], self._body())
                return self._send(200 if rec else 404, rec or {"error": "not found"})
            if p == ["api", "escalations"]:
                return self._send(201, plat.create_escalation(self._body()))
            if p == ["api", "fleet", "incident"]:
                plat.fleet.trigger_incident()
                log(f"fleet incident triggered on {INCIDENT_JOB}")
                return self._send(200, {"triggered": INCIDENT_JOB})
            if p == ["api", "reset"]:
                plat.reset()
                return self._send(200, {"reset": True})
        except (KeyError, ValueError) as e:
            return self._send(400, {"error": str(e)})
        self._send(404, {"error": "not found"})


def main():
    platform = Platform()
    Handler.platform = platform
    threading.Thread(target=platform.collect_loop, daemon=True, name="collector").start()
    threading.Thread(target=platform.records_loop, daemon=True, name="records").start()
    server = ThreadingHTTPServer(("localhost", PORT), Handler)
    log(f"platform-sim on http://localhost:{PORT} (target {target_ctl.REST}, Kafka {KAFKA})")
    server.serve_forever()


if __name__ == "__main__":
    main()
