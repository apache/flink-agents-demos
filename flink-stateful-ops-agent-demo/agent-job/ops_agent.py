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
"""The operations agent: one instance of memory per watched job (Flink keyed state).

Per window the agent receives:
  observe   -> cooldown / healthy / WAIT are decided in code; otherwise ask the model
  decide    -> the model's Decision, checked against what the runbook allows right now
  apply     -> durable redeploy through the platform API (exactly once, even across crashes)
  escalate  -> durable escalation with a report written from memory
Every step emits a record (decision + memory snapshot) to Kafka for the dashboard.
"""
import functools
import json
import os
import time
from pathlib import Path

from flink_agents.api.agents.agent import STRUCTURED_OUTPUT, Agent
from flink_agents.api.agents.types import OutputSchema
from flink_agents.api.chat_message import ChatMessage, MessageRole
from flink_agents.api.decorators import action, chat_model_connection, chat_model_setup
from flink_agents.api.events.chat_event import ChatRequestEvent, ChatResponseEvent
from flink_agents.api.events.event import Event, InputEvent, OutputEvent
from flink_agents.api.resource import ResourceDescriptor, ResourceName
from flink_agents.api.runner_context import RunnerContext

import ops_tools
from ops_runbook import (
    MAX_INCREASES,
    PERSIST_WINDOWS,
    Decision,
    allowed_actions,
    baseline_from,
    escalation_report,
    new_memory,
    next_interval,
    observation,
    open_incident,
    rule_decision,
    system_prompt,
    user_prompt,
)

RUNBOOK_PATH = "ops.runbook-path"
COOLDOWN_SECONDS = "ops.cooldown-seconds"
OLLAMA_URL = os.environ.get("OLLAMA_URL", "http://localhost:11434")
OLLAMA_MODEL = os.environ.get("OLLAMA_MODEL", "qwen3:8b")
MEMORY_FIELDS = ("incident", "history", "incident_seq", "tasks_reverted", "escalated", "windows")
WINDOW_FIELDS = ("window_end", "running", "anomalous", "skewed_windows", "config", "tasks_per_tm",
                 "cpu", "task_skew", "backpressured", "records_per_sec", "recent_failovers")


def _load(ctx: RunnerContext) -> dict:
    mem = new_memory()
    mem["windows"] = []
    for field in MEMORY_FIELDS:
        value = ctx.short_term_memory.get(field)
        if value is not None:
            mem[field] = value
    return mem


def _save(ctx: RunnerContext, mem: dict) -> None:
    for field in MEMORY_FIELDS:
        ctx.short_term_memory.set(field, mem.get(field))


def _emit(ctx: RunnerContext, kind: str, window: dict, mem: dict, key: str, **extra) -> None:
    record = {
        "id": f"{window['job']}|{key}|{kind}",
        "kind": kind,
        "job": window["job"],
        "simulated": window.get("simulated", False),
        "at": time.time(),
        "window": {k: window.get(k) for k in WINDOW_FIELDS},
        "memory": {k: mem.get(k) for k in ("incident", "history", "tasks_reverted", "escalated")},
        **extra,
    }
    ctx.send_event(OutputEvent(output=json.dumps(record)))


def _close(mem: dict, outcome: str, at: float) -> None:
    incident = mem["incident"]
    mem["history"] = (mem.get("history") or []) + [{
        "id": incident["id"], "outcome": outcome, "closed_at": at,
        "baseline": incident.get("baseline"), "actions": [a["label"] for a in incident["attempts"]],
    }]
    mem["history"] = mem["history"][-5:]
    if outcome == "REVERTED":
        mem["tasks_reverted"] = True
    mem["incident"] = None


def _apply_decision(ctx: RunnerContext, mem: dict, window: dict, decision: Decision, **info) -> None:
    """Turn a checked decision into memory updates and, if needed, a side effect."""
    job, incident, config = window["job"], mem["incident"], window.get("config") or {}
    attempts = incident["attempts"]
    if attempts and "observed_after" not in attempts[-1]:
        attempts[-1]["observed_after"] = observation(window)
    target, label = None, decision.action
    if decision.action == "APPLY_TASKS":
        incident["baseline"] = baseline_from(mem["windows"])
        target = {"mode": "TASKS", "interval_ms": config.get("interval_ms", 20)}
    elif decision.action == "INCREASE_INTERVAL":
        incident["baseline"] = incident.get("baseline") or baseline_from(mem["windows"])
        incident["increases"] += 1
        target = {"mode": config.get("mode"), "interval_ms": next_interval(window)}
        label = f"INCREASE_INTERVAL {target['interval_ms']}"
    elif decision.action == "REVERT":
        target = incident["config_before"]

    record = {"decision": decision.model_dump(), **info}
    if target:
        request_id = f"{job}#{incident['id']}.{len(attempts) + 1}"
        attempts.append({"action": decision.action, "label": label, "target": target,
                         "request_id": request_id, "decided_at": window["window_end"]})
        incident["phase"] = "applying"
        record["request_id"] = request_id
        ctx.send_event(Event(type="apply_config", attributes={
            "job": job, "request_id": request_id, "mode": target["mode"],
            "interval_ms": target["interval_ms"], "reason": f"{label} ({decision.runbook_step})",
        }))
    elif decision.action == "KEEP":
        _close(mem, "KEPT", window["window_end"])
    elif decision.action == "ESCALATE":
        incident["phase"] = "escalating"
        request_id = f"{job}#{incident['id']}.escalation"
        ctx.send_event(Event(type="escalate", attributes={
            "job": job, "request_id": request_id,
            "title": f"FLINK-38715: unbalanced task placement persists for {job}",
            "body": escalation_report(job, mem, window),
        }))
    elif decision.action == "NO_ACTION":
        _close(mem, "NO_ACTION", window["window_end"])
    _emit(ctx, "decision", window, mem, str(window["window_end"]), **record)


class OpsAgent(Agent):
    @chat_model_connection
    @staticmethod
    def ollama() -> ResourceDescriptor:
        return ResourceDescriptor(
            clazz=ResourceName.ChatModel.OLLAMA_CONNECTION, base_url=OLLAMA_URL, request_timeout=180.0
        )

    @chat_model_setup
    @staticmethod
    def runbook_llm() -> ResourceDescriptor:
        return ResourceDescriptor(
            clazz=ResourceName.ChatModel.OLLAMA_SETUP, connection="ollama", model=OLLAMA_MODEL,
            temperature=0.0, num_ctx=8192, think=False, keep_alive="60m",
        )

    @action(InputEvent.EVENT_TYPE)
    @staticmethod
    def observe(event: Event, ctx: RunnerContext) -> None:
        window = InputEvent.from_event(event).input
        mem = _load(ctx)
        mem["windows"] = (mem["windows"] + [{k: window.get(k) for k in WINDOW_FIELDS}])[-6:]
        ctx.sensory_memory.set("window", window)
        incident, now, key = mem["incident"], window["window_end"], str(window["window_end"])

        if incident and incident.get("cooldown_until") and now < incident["cooldown_until"]:
            _emit(ctx, "observe", window, mem, key,
                  note=f"cooldown: verify in {round(incident['cooldown_until'] - now)}s")
        elif incident is None and not window["anomalous"]:
            mem["escalated"] = None
            _emit(ctx, "observe", window, mem, key, note="healthy")
        elif incident is None and mem.get("escalated"):
            # Handed to a human: keep watching, change nothing until the job is healthy again.
            _emit(ctx, "observe", window, mem, key,
                  note=f"escalated as {mem['escalated']['request_id']}: hands off, waiting for a human")
        elif incident is None and window["skewed_windows"] < PERSIST_WINDOWS:
            _emit(ctx, "observe", window, mem, key, rule="WAIT",
                  note=f"skewed {window['skewed_windows']}/{PERSIST_WINDOWS} windows: WAIT (runbook step 1)")
        elif incident is not None and not (window["running"] and window.get("records_per_sec")):
            _emit(ctx, "observe", window, mem, key, note="waiting for the job to run again")
        elif incident is not None and incident["increases"] >= MAX_INCREASES and window["anomalous"]:
            _apply_decision(ctx, mem, window, Decision(
                runbook_step="Step 5: still unbalanced after 3 increases",
                rationale=f"Guardrail in code: {MAX_INCREASES} increases done, skew is still "
                          f"{window.get('task_skew')}.",
                action="ESCALATE"), source="guardrail")
        else:
            if incident is None:
                open_incident(mem, window)
            runbook = Path(ctx.config.get_str(RUNBOOK_PATH)).read_text()
            ctx.sensory_memory.set("asked_at", time.time())
            ctx.send_event(ChatRequestEvent(
                model="runbook_llm",
                messages=[
                    ChatMessage.of(MessageRole.SYSTEM, system_prompt(runbook)),
                    ChatMessage.of(MessageRole.USER, user_prompt(window["job"], mem, window)),
                ],
                output_schema=OutputSchema(output_schema=Decision),
            ))
        _save(ctx, mem)

    @action(ChatResponseEvent.EVENT_TYPE)
    @staticmethod
    def decide(event: Event, ctx: RunnerContext) -> None:
        response = ChatResponseEvent.from_event(event)
        window = ctx.sensory_memory.get("window")
        mem = _load(ctx)
        allowed = allowed_actions(mem, window)
        decision, note = None, None
        if response.is_success:
            try:
                parsed = response.response.extra_args.get(STRUCTURED_OUTPUT)
                decision = parsed if isinstance(parsed, Decision) else Decision.model_validate(parsed)
            except (ValueError, TypeError) as e:
                note = f"unparseable model reply: {e}"[:200]
        else:
            note = f"model call failed: {response.error}"[:200]
        source = "llm"
        if decision is not None and decision.action not in allowed:
            note = f"model chose {decision.action}; allowed now: {', '.join(allowed)}"
            decision = None
        if decision is None:
            decision, source = rule_decision(mem, window), "guardrail"
        _apply_decision(ctx, mem, window, decision, source=source, note=note, allowed=allowed,
                        llm_seconds=round(time.time() - ctx.sensory_memory.get("asked_at"), 1))
        _save(ctx, mem)

    @action("apply_config")
    @staticmethod
    async def apply(event: Event, ctx: RunnerContext) -> None:
        args = [event.get_attr(k) for k in ("job", "request_id", "mode", "interval_ms", "reason")]
        request_id = args[1]
        receipt = await ctx.durable_execute_async(
            ops_tools.redeploy, *args,
            reconciler=functools.partial(ops_tools.reconcile_redeploy, *args),
            durable_id=f"redeploy:{request_id}",
        )
        window = ctx.sensory_memory.get("window")
        mem = _load(ctx)
        incident = mem["incident"]
        attempt = next(a for a in incident["attempts"] if a["request_id"] == request_id)
        attempt["took_s"] = receipt.get("took_s")
        attempt["reconciled"] = bool(receipt.get("reconciled"))
        if attempt["action"] == "REVERT":
            _close(mem, "REVERTED", time.time())
        else:
            incident["phase"] = "verifying"
            incident["cooldown_until"] = time.time() + ctx.config.get_int(COOLDOWN_SECONDS, 30)
        _save(ctx, mem)
        _emit(ctx, "applied", window, mem, request_id, request_id=request_id, receipt=receipt)

    @action("escalate")
    @staticmethod
    async def escalate(event: Event, ctx: RunnerContext) -> None:
        args = [event.get_attr(k) for k in ("job", "request_id", "title", "body")]
        receipt = await ctx.durable_execute_async(
            ops_tools.file_escalation, *args,
            reconciler=functools.partial(ops_tools.reconcile_escalation, *args),
            durable_id=f"escalation:{args[1]}",
        )
        window = ctx.sensory_memory.get("window")
        mem = _load(ctx)
        _close(mem, "ESCALATED", time.time())
        mem["escalated"] = {"request_id": args[1], "at": time.time()}
        _save(ctx, mem)
        _emit(ctx, "escalated", window, mem, args[1], request_id=args[1], receipt=receipt,
              title=args[2], report=args[3])
