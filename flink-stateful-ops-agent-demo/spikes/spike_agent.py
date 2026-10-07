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
"""P0 spike: short-term memory across runs + durable side effects across a crash.

Each input {"job": "...", "seq": N, "reconcile": true|false,
"apply_secs": A, "observe_secs": O} for one job (durations default to 20s):
  1. reads a per-job run counter from short-term memory (keyed state),
  2. APPLY: durable side effect (writes a ledger line, then takes APPLY_SECONDS,
     like a redeploy that is in flight); optionally with a reconciler,
  3. OBSERVE: durable slow step (OBSERVE_SECONDS, like waiting for metrics),
  4. updates memory and emits the result.
Both slow steps use durable_execute_async, so other jobs (keys) can proceed
on the same subtask meanwhile.

Chaos checks (kill -9 the TaskManager whose pid is in the ledger line):
  - kill during APPLY with reconcile=true  -> reconciler finds the ledger line,
    no second ledger line for (job, seq)
  - kill during APPLY with reconcile=false -> the side effect runs again
    (baseline: what a naive retry does)
  - kill during OBSERVE -> APPLY is replayed from the action store, not re-run
  - in all cases the run counter continues -> memory survived
"""
import json
import os
import time

from flink_agents.api.agents.agent import Agent
from flink_agents.api.decorators import action
from flink_agents.api.events.event import Event, InputEvent, OutputEvent
from flink_agents.api.runner_context import RunnerContext

OUT_DIR = os.environ.get("SPIKE_DIR", "/tmp/stateful_ops_spike")
LEDGER = os.path.join(OUT_DIR, "side_effect_ledger.log")
RECONCILE_LOG = os.path.join(OUT_DIR, "reconcile.log")
APPLY_SECONDS = 20
OBSERVE_SECONDS = 20


def _log(path: str, line: str) -> None:
    os.makedirs(OUT_DIR, exist_ok=True)
    with open(path, "a") as f:
        f.write(f"{time.strftime('%H:%M:%S')} {line}\n")


def _apply(job: str, seq: int, run: int, secs: int = APPLY_SECONDS) -> str:
    receipt = f"job={job} seq={seq} run={run} pid={os.getpid()}"
    _log(LEDGER, receipt)
    time.sleep(secs)
    return receipt


def _reconcile(job: str, seq: int, run: int, secs: int = APPLY_SECONDS) -> str:
    """Recovery-only: did the side effect already happen before the crash?"""
    tag = f"job={job} seq={seq} "
    if os.path.exists(LEDGER):
        with open(LEDGER) as f:
            for line in f:
                if tag in line:
                    _log(RECONCILE_LOG, f"{tag}found existing -> skip re-apply")
                    return line.strip().split(" ", 1)[1]
    _log(RECONCILE_LOG, f"{tag}not found -> apply now")
    return _apply(job, seq, run, secs)


def _observe(job: str, seq: int, secs: int = OBSERVE_SECONDS) -> str:
    time.sleep(secs)
    return "healthy"


class SpikeAgent(Agent):
    @action(InputEvent.EVENT_TYPE)
    @staticmethod
    async def handle(event: Event, ctx: RunnerContext) -> None:
        raw = InputEvent.from_event(event).input
        data = raw if isinstance(raw, dict) else json.loads(raw)
        job, seq = data["job"], int(data["seq"])
        use_reconciler = bool(data.get("reconcile", True))
        apply_secs = int(data.get("apply_secs", APPLY_SECONDS))
        observe_secs = int(data.get("observe_secs", OBSERVE_SECONDS))
        stm = ctx.short_term_memory
        runs = (stm.get("runs") or 0) + 1
        history = stm.get("history") or []
        started = time.strftime("%H:%M:%S")

        if use_reconciler:
            receipt = await ctx.durable_execute_async(
                _apply, job, seq, runs, apply_secs,
                reconciler=lambda: _reconcile(job, seq, runs, apply_secs),
            )
        else:
            receipt = await ctx.durable_execute_async(_apply, job, seq, runs, apply_secs)
        verdict = await ctx.durable_execute_async(_observe, job, seq, observe_secs)

        history.append(seq)
        stm.set("runs", runs)
        stm.set("history", history)
        ctx.send_event(
            OutputEvent(
                output=json.dumps(
                    {"job": job, "seq": seq, "runs": runs, "history": history,
                     "receipt": receipt, "verdict": verdict, "started": started,
                     "finished": time.strftime("%H:%M:%S")}
                )
            )
        )
