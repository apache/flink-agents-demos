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
"""One metric sample of the REAL target job, read from the target cluster's REST API.

The REST metric store answers from its previous fetch, so values are a few seconds
old; that is fine for windows of 15s.
"""
import json
import urllib.request

import target_ctl

TIMEOUT_S = 2


def _get(path):
    with urllib.request.urlopen(target_ctl.REST + path, timeout=TIMEOUT_S) as resp:
        return json.loads(resp.read() or b"{}")


def _agg(jid, vid, metric, agg):
    values = _get(f"/jobs/{jid}/vertices/{vid}/subtasks/metrics?get={metric}&agg={agg}")
    return float(values[0][agg]) if values else None


def _parse_ms(value):
    value = (value or "20ms").strip()
    if value.endswith("ms"):
        return int(float(value[:-2]))
    if value.endswith("s"):
        return int(float(value[:-1]) * 1000)
    return int(float(value))


def sample(job_key, ts):
    s = {"job": job_key, "simulated": False, "ts": ts, "reachable": False, "running": False}
    try:
        cfg = {e["key"]: e["value"] for e in _get("/jobmanager/config")}
        s["reachable"] = True
        s["config"] = {"mode": cfg.get("taskmanager.load-balance.mode", "NONE"),
                       "interval_ms": _parse_ms(cfg.get("slot.request.max-interval"))}
        jid = next((j["jid"] for j in _get("/jobs/overview")["jobs"]
                    if j["name"] == target_ctl.JOB_NAME and j["state"] == "RUNNING"), None)
        if not jid:
            return s
        s["job_id"] = jid
        vertices = _get(f"/jobs/{jid}")["vertices"]
        if not all(v["status"] == "RUNNING" for v in vertices):
            return s
        tms = sorted(tm["id"] for tm in _get("/taskmanagers")["taskmanagers"])
        tasks = {tm: 0 for tm in tms}
        for v in vertices:
            for st in _get(f"/jobs/{jid}/vertices/{v['id']}")["subtasks"]:
                if st["taskmanager-id"] not in tasks:
                    return s
                tasks[st["taskmanager-id"]] += 1
        cpu = {}
        for tm in tms:
            values = _get(f"/taskmanagers/{tm}/metrics?get=Status.JVM.CPU.Load")
            if not values:
                return s
            cpu[tm] = round(float(values[0]["value"]), 3)
        backpressure = _agg(jid, vertices[0]["id"], "backPressuredTimeMsPerSecond", "max")
        records_in = _agg(jid, vertices[-1]["id"], "numRecordsIn", "sum")
        if backpressure is None or records_in is None:
            return s
        s.update(running=True, tasks_per_tm=tasks, cpu=cpu, backpressure_ms=round(backpressure),
                 records_in=records_in, recent_failovers=0)
    except (OSError, ValueError, KeyError):
        pass
    return s
