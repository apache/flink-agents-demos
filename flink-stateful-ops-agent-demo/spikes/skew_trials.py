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
"""P0 skew spike: alternate redeploys between scheduling modes and record layout + load."""
import json
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "target-cluster"))
import target_ctl as t  # noqa: E402

WARMUP_S = int(os.environ.get("WARMUP_S", "60"))


def summary(label, took):
    s = t.status()
    tms = s["taskmanagers"]
    row = {
        "label": label,
        "redeploy_s": round(took, 1),
        "config": s["config"],
        "tasks_per_tm": {k: v["num_tasks"] for k, v in tms.items()},
        "cpu_per_tm": {k: round(v["cpu_load"] or 0, 2) for k, v in tms.items()},
        "skew": s["task_skew"],
        "rps": round(s["records_per_sec"] or 0),
    }
    print(json.dumps(row), flush=True)
    return row


for mode in sys.argv[1:]:
    start = time.time()
    t.redeploy(mode)
    took = time.time() - start
    time.sleep(WARMUP_S)
    summary(mode, took)
