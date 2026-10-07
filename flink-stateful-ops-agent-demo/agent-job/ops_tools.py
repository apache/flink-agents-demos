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
"""Side effects of the agent, called through durable execution.

Each function talks to the platform's deployment API (platform-sim stands in for
it). The platform executes every request it receives; it does not de-duplicate.
Exactly-once comes from the agent: a completed call is replayed from the action
store on recovery, and an in-flight call goes through the reconciler, which asks
the platform whether the request already exists instead of sending it again.
"""
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request

PLATFORM_URL = os.environ.get("OPS_PLATFORM_URL", "http://localhost:8090")
DEPLOY_TIMEOUT_S = 300


def _call(method, path, body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        PLATFORM_URL + path, data=data, method=method, headers={"Content-Type": "application/json"}
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read() or b"{}")
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None
        raise


def _id(request_id):
    return urllib.parse.quote(request_id, safe="")


def _wait_done(request_id):
    deadline = time.time() + DEPLOY_TIMEOUT_S
    while time.time() < deadline:
        dep = _call("GET", f"/api/deployments/{_id(request_id)}")
        if dep and dep["status"] == "done":
            return {k: dep.get(k) for k in ("request_id", "job", "target", "result", "took_s")}
        if dep and dep["status"] == "failed":
            raise RuntimeError(f"redeploy {request_id} failed: {dep.get('error')}")
        time.sleep(1)
    raise TimeoutError(f"redeploy {request_id} did not finish in {DEPLOY_TIMEOUT_S}s")


def redeploy(job, request_id, mode, interval_ms, reason):
    """Ask the platform to apply a scheduling config (savepoint, recreate, restore)."""
    _call("POST", "/api/deployments", {
        "request_id": request_id, "job": job, "mode": mode, "interval_ms": interval_ms,
        "reason": reason, "agent_pid": os.getpid(),
    })
    return _wait_done(request_id)


def reconcile_redeploy(job, request_id, mode, interval_ms, reason):
    """Recovery only: the call was in flight when the agent crashed. Did it reach the platform?"""
    if _call("GET", f"/api/deployments/{_id(request_id)}") is None:
        return redeploy(job, request_id, mode, interval_ms, reason)
    _call("POST", f"/api/deployments/{_id(request_id)}/reconciled", {"agent_pid": os.getpid()})
    receipt = _wait_done(request_id)
    receipt["reconciled"] = True
    return receipt


def file_escalation(job, request_id, title, body):
    """Post the escalation (in production: a JIRA comment or a page)."""
    _call("POST", "/api/escalations", {"request_id": request_id, "job": job, "title": title, "body": body})
    return {"request_id": request_id, "filed": True}


def reconcile_escalation(job, request_id, title, body):
    if _call("GET", f"/api/escalations/{_id(request_id)}") is None:
        return file_escalation(job, request_id, title, body)
    return {"request_id": request_id, "filed": True, "reconciled": True}
