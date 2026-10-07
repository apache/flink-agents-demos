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
"""The runbook decision and the guardrails around it (no Flink imports).

The model picks the runbook step; code decides which actions are allowed at that
point of the incident, computes the new interval, and enforces the limits.
"""
import json
from typing import Literal

from pydantic import BaseModel

PERSIST_WINDOWS = 3
MAX_INCREASES = 3
INTERVAL_STEP_MS = 50

Action = Literal["WAIT", "APPLY_TASKS", "KEEP", "REVERT", "INCREASE_INTERVAL", "ESCALATE", "NO_ACTION"]


class Decision(BaseModel):
    runbook_step: str
    rationale: str
    action: Action
    interval_ms: int | None = None


OUTPUT_FORMAT = (
    "Decide the single next step for this job. Reply with ONLY a JSON object, no prose, "
    "with the fields in this order:\n"
    '{"runbook_step": "<the matching row of the runbook decision table>", '
    '"rationale": "<one sentence: which facts from memory and the observation match that row>", '
    '"action": "WAIT" | "APPLY_TASKS" | "KEEP" | "REVERT" | "INCREASE_INTERVAL" | '
    '"ESCALATE" | "NO_ACTION", "interval_ms": <new slot.request.max-interval in ms, '
    "only for INCREASE_INTERVAL, else null>}"
)


def system_prompt(runbook: str) -> str:
    return (
        "You are an on-call operations agent for Apache Flink jobs. The memory section is "
        "what you observed and did earlier for this job. Follow this runbook strictly:\n\n"
        + runbook + "\n\n" + OUTPUT_FORMAT
    )


def new_memory() -> dict:
    return {"incident": None, "history": [], "incident_seq": 0, "tasks_reverted": False,
            "escalated": None}


def observation(window: dict) -> dict:
    """The facts from one window that the model sees."""
    return {
        "tasks_per_tm": window.get("tasks_per_tm"),
        "cpu": window.get("cpu"),
        "backpressured": window.get("backpressured"),
        "records_per_sec": window.get("records_per_sec"),
        "recent_failovers": window.get("recent_failovers", 0),
    }


def open_incident(mem: dict, window: dict) -> dict:
    mem["incident_seq"] = mem.get("incident_seq", 0) + 1
    incident = {
        "id": mem["incident_seq"],
        "opened_at": window["window_end"],
        "config_before": window.get("config"),
        "baseline": None,
        "attempts": [],
        "increases": 0,
        "phase": "deciding",
        "cooldown_until": None,
    }
    mem["incident"] = incident
    return incident


def baseline_from(windows: list) -> dict:
    """Throughput and placement from the last skewed windows, before any change."""
    skewed = [w for w in windows if w.get("anomalous")][-PERSIST_WINDOWS:]
    rates = [w["records_per_sec"] for w in skewed if w.get("records_per_sec")]
    last = skewed[-1] if skewed else windows[-1]
    return {
        "records_per_sec": round(sum(rates) / len(rates)) if rates else None,
        "tasks_per_tm": last.get("tasks_per_tm"),
        "cpu": last.get("cpu"),
        "skew": last.get("task_skew"),
    }


def allowed_actions(mem: dict, window: dict) -> list:
    incident = mem.get("incident") or {}
    attempts = incident.get("attempts") or []
    mode = (window.get("config") or {}).get("mode")
    last = attempts[-1]["action"] if attempts else None
    if last is None:
        if mode == "NONE":
            return ["ESCALATE"] if mem.get("tasks_reverted") else ["APPLY_TASKS", "WAIT", "NO_ACTION"]
        return ["INCREASE_INTERVAL", "WAIT", "NO_ACTION"]
    if last == "APPLY_TASKS":
        return ["KEEP", "REVERT"]
    if last == "INCREASE_INTERVAL":
        if not window.get("anomalous"):
            return ["KEEP"]
        # Decision table: ESCALATE only once the increases are used up.
        if incident.get("increases", 0) >= MAX_INCREASES:
            return ["ESCALATE"]
        return ["INCREASE_INTERVAL"]
    return ["ESCALATE"]


def rule_decision(mem: dict, window: dict) -> Decision:
    """What the runbook table says, computed in code; used when the model's answer is
    missing or not allowed at this point."""
    allowed = allowed_actions(mem, window)
    incident = mem.get("incident") or {}
    baseline = (incident.get("baseline") or {}).get("records_per_sec")
    rate = window.get("records_per_sec")
    if "APPLY_TASKS" in allowed:
        return Decision(runbook_step="Step 2", rationale="Skew persisted, TASKS not tried yet.",
                        action="APPLY_TASKS")
    if allowed == ["KEEP", "REVERT"]:
        worse = baseline and rate is not None and rate < baseline
        return Decision(runbook_step="Step 3", rationale="Compared with the baseline.",
                        action="REVERT" if worse else "KEEP")
    if "INCREASE_INTERVAL" in allowed:
        return Decision(runbook_step="Step 4", rationale="TASKS on and the skew is back.",
                        action="INCREASE_INTERVAL")
    return Decision(runbook_step="Step 5" if allowed == ["ESCALATE"] else "Step 3",
                    rationale="Only allowed action at this point.", action=allowed[0])


def prompt_memory(mem: dict, window: dict) -> dict:
    """Memory as the model sees it: the facts that select a row of the decision table."""
    incident = mem.get("incident") or {}
    attempts = incident.get("attempts") or []
    view = {
        "incident": "open",
        "windows_with_skew": window.get("skewed_windows", 0),
        "config": window.get("config"),
        "baseline": incident.get("baseline"),
        "actions": [a["label"] for a in attempts],
    }
    if attempts:
        view["cooldown_over"] = True
    if not attempts and (window.get("config") or {}).get("mode") == "TASKS":
        view["since_last_action"] = (
            f"TASKS was already on; {window.get('recent_failovers', 0)} failovers; "
            f"skew back for {window.get('skewed_windows', 0)} windows"
        )
    if attempts and attempts[-1]["action"] == "INCREASE_INTERVAL":
        view["increases_so_far"] = incident.get("increases", 0)
        view["increases_allowed"] = MAX_INCREASES
        view["after_each_increase"] = (
            f"still skew {window.get('task_skew')} after redeploy" if window.get("anomalous")
            else "balanced after redeploy"
        )
    return view


def user_prompt(job: str, mem: dict, window: dict) -> str:
    return (
        f"Job: {job}\nMemory: {json.dumps(prompt_memory(mem, window))}\n"
        f"Latest observation: {json.dumps(observation(window))}\n"
        f"Allowed actions now: {', '.join(allowed_actions(mem, window))}"
    )


def next_interval(window: dict) -> int:
    return int((window.get("config") or {}).get("interval_ms") or 20) + INTERVAL_STEP_MS


def _cfg(config: dict | None) -> str:
    if not config:
        return "n/a"
    return f"mode={config.get('mode')}, max-interval={config.get('interval_ms')}ms"


def _per_tm(values: dict | None, pct: bool = False) -> str:
    if not values:
        return "n/a"
    return " / ".join(f"{tm} {round(v * 100)}%" if pct else f"{tm}: {v}"
                      for tm, v in sorted(values.items()))


def escalation_report(job: str, mem: dict, window: dict) -> str:
    """The FLINK-38715 comment, written from the agent's memory."""
    incident = mem["incident"]
    base = incident.get("baseline") or {}
    lines = [
        f"### Unbalanced task placement persists: {job}",
        "",
        "Runbook: balanced-task-scheduling (Flink 2.2, FLIP-370), steps 1-5 followed by "
        "the Flink Agents operations agent.",
        "",
        f"- Config before: `{_cfg(incident.get('config_before'))}`",
        f"- Config now: `{_cfg(window.get('config'))}`",
        f"- Baseline: {base.get('records_per_sec') or 'n/a'} rec/s, tasks per TM "
        f"`{_per_tm(base.get('tasks_per_tm'))}`" if base else "- Baseline: not recorded",
        f"- Recent failovers: {window.get('recent_failovers', 0)}",
        "",
        "| # | Action | Config after | Tasks per TM after | Rec/s after |",
        "|---|---|---|---|---|",
    ]
    for i, a in enumerate(incident.get("attempts") or [], 1):
        after = a.get("observed_after") or {}
        lines.append(
            f"| {i} | {a['label']} | {_cfg(a.get('target'))} | "
            f"{_per_tm(after.get('tasks_per_tm'))} | {after.get('records_per_sec', 'n/a')} |"
        )
    lines += [
        "",
        f"Latest: tasks per TM `{_per_tm(window.get('tasks_per_tm'))}`, CPU "
        f"`{_per_tm(window.get('cpu'), pct=True)}`, {window.get('records_per_sec')} rec/s.",
        f"Stopped after {incident.get('increases', 0)} interval increases (limit {MAX_INCREASES}).",
    ]
    return "\n".join(lines)
