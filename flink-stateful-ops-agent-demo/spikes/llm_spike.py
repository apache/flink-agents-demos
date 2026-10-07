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
"""P0 spike: can a small local model follow the runbook and return a valid Decision?

Runs 7 runbook scenarios through a flink-agents agent on a PyFlink mini-cluster,
in two variants:
  skill   the runbook is a Skill the model must load with the load_skill tool
  inline  the runbook text is in the system prompt
Each result (action, expected action, latency, retries) is appended to OUT.

Usage: cd spikes && python -c 'import llm_spike; llm_spike.main()' [skill|inline|both] [repetitions]
"""
import json
import os
import sys
import sysconfig
import time
from pathlib import Path
from typing import Literal

from pydantic import BaseModel
from pyflink.common import Configuration
from pyflink.datastream import StreamExecutionEnvironment

from flink_agents.api.agents.agent import STRUCTURED_OUTPUT, Agent
from flink_agents.api.agents.types import OutputSchema
from flink_agents.api.chat_message import ChatMessage, MessageRole
from flink_agents.api.core_options import AgentConfigOptions, AgentExecutionOptions
from flink_agents.api.decorators import action, chat_model_connection, chat_model_setup, skills
from flink_agents.api.events.chat_event import ChatRequestEvent, ChatResponseEvent
from flink_agents.api.events.event import Event, InputEvent
from flink_agents.api.events.event_type import EventType
from flink_agents.api.execution_environment import AgentsExecutionEnvironment
from flink_agents.api.resource import ResourceDescriptor, ResourceName
from flink_agents.api.runner_context import RunnerContext
from flink_agents.api.skills import Skills

MODULE_DIR = Path(__file__).resolve().parent.parent
SKILLS_DIR = MODULE_DIR / "skills"
RUNBOOK = (SKILLS_DIR / "balanced-task-scheduling" / "SKILL.md").read_text()
OUT = os.environ.get("LLM_SPIKE_OUT", "/tmp/stateful_ops_spike/llm_spike.jsonl")
EVENT_LOG_DIR = "/tmp/stateful_ops_spike/eventlog"
MODEL = os.environ.get("OLLAMA_MODEL", "qwen3:8b")
THINK = os.environ.get("OLLAMA_THINK", "false") == "true"
# native: output_schema (Ollama's JSON format; blocks tool calls). prompt: parse the reply ourselves.
SCHEMA = os.environ.get("SCHEMA", "native")


def parse_decision(response) -> "Decision":
    decision = response.extra_args.get(STRUCTURED_OUTPUT)
    if decision is None:
        text = response.text.strip()
        decision = text[text.find("{"): text.rfind("}") + 1]
        return Decision.model_validate_json(decision)
    return decision if isinstance(decision, Decision) else Decision.model_validate(decision)

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
SYSTEM_SKILL = (
    "You are an on-call operations agent for Apache Flink jobs. The memory section is "
    "what you observed and did earlier for this job. Load the balanced-task-scheduling "
    "skill first and follow it strictly.\n" + OUTPUT_FORMAT
)
SYSTEM_INLINE = (
    "You are an on-call operations agent for Apache Flink jobs. The memory section is "
    "what you observed and did earlier for this job. Follow this runbook strictly:\n\n"
    + RUNBOOK + "\n\n" + OUTPUT_FORMAT
)

# (name, expected action, memory, latest observation)
SCENARIOS = [
    ("first-window", "WAIT",
     {"incident": None, "windows_with_skew": 1, "config": {"mode": "NONE", "interval_ms": 20}},
     {"tasks_per_tm": {"tm-1": 4, "tm-2": 3, "tm-3": 2}, "cpu": {"tm-1": 1.0, "tm-2": 0.68, "tm-3": 0.27},
      "backpressured": True, "records_per_sec": 17000}),
    ("persistent-skew", "APPLY_TASKS",
     {"incident": "open", "windows_with_skew": 3, "config": {"mode": "NONE", "interval_ms": 20},
      "baseline": None, "actions": []},
     {"tasks_per_tm": {"tm-1": 4, "tm-2": 3, "tm-3": 2}, "cpu": {"tm-1": 1.0, "tm-2": 0.68, "tm-3": 0.27},
      "backpressured": True, "records_per_sec": 17000}),
    ("verify-improved", "KEEP",
     {"incident": "open", "config": {"mode": "TASKS", "interval_ms": 20},
      "baseline": {"skew": 2, "records_per_sec": 17000}, "actions": ["APPLY_TASKS"],
      "cooldown_over": True},
     {"tasks_per_tm": {"tm-1": 3, "tm-2": 3, "tm-3": 3}, "cpu": {"tm-1": 1.0, "tm-2": 1.0, "tm-3": 1.0},
      "backpressured": False, "records_per_sec": 28000}),
    ("verify-worse", "REVERT",
     {"incident": "open", "config": {"mode": "TASKS", "interval_ms": 20},
      "baseline": {"skew": 2, "records_per_sec": 17000}, "actions": ["APPLY_TASKS"],
      "cooldown_over": True},
     {"tasks_per_tm": {"tm-1": 3, "tm-2": 3, "tm-3": 3}, "cpu": {"tm-1": 0.6, "tm-2": 0.6, "tm-3": 0.6},
      "backpressured": False, "records_per_sec": 12000}),
    ("unbalanced-after-failover", "INCREASE_INTERVAL",
     {"incident": "open", "config": {"mode": "TASKS", "interval_ms": 20},
      "baseline": {"skew": 2, "records_per_sec": 17000}, "actions": ["APPLY_TASKS", "KEEP"],
      "since_last_action": "2 failovers; skew back for 3 windows", "cooldown_over": True},
     {"tasks_per_tm": {"tm-1": 4, "tm-2": 3, "tm-3": 2}, "cpu": {"tm-1": 1.0, "tm-2": 0.7, "tm-3": 0.3},
      "backpressured": True, "records_per_sec": 18000}),
    ("increases-exhausted", "ESCALATE",
     {"incident": "open", "config": {"mode": "TASKS", "interval_ms": 170},
      "baseline": {"skew": 2, "records_per_sec": 17000},
      "actions": ["APPLY_TASKS", "KEEP", "INCREASE_INTERVAL 70", "INCREASE_INTERVAL 120",
                  "INCREASE_INTERVAL 170"],
      "after_each_increase": "still skew 2 after failover", "cooldown_over": True},
     {"tasks_per_tm": {"tm-1": 4, "tm-2": 3, "tm-3": 2}, "cpu": {"tm-1": 1.0, "tm-2": 0.7, "tm-3": 0.3},
      "backpressured": True, "records_per_sec": 18000}),
    ("healthy", "NO_ACTION",
     {"incident": None, "windows_with_skew": 0, "config": {"mode": "NONE", "interval_ms": 20}},
     {"tasks_per_tm": {"tm-1": 3, "tm-2": 3, "tm-3": 3}, "cpu": {"tm-1": 0.4, "tm-2": 0.4, "tm-3": 0.4},
      "backpressured": False, "records_per_sec": 9000}),
]


class SpikeLlmAgent(Agent):
    @chat_model_connection
    @staticmethod
    def ollama() -> ResourceDescriptor:
        return ResourceDescriptor(
            clazz=ResourceName.ChatModel.OLLAMA_CONNECTION,
            base_url="http://localhost:11434",
            request_timeout=180.0,
        )

    @chat_model_setup
    @staticmethod
    def with_skill() -> ResourceDescriptor:
        return ResourceDescriptor(
            clazz=ResourceName.ChatModel.OLLAMA_SETUP, connection="ollama", model=MODEL,
            temperature=0.0, num_ctx=8192, think=THINK, keep_alive="30m",
            skills=["balanced-task-scheduling"],
        )

    @chat_model_setup
    @staticmethod
    def inline() -> ResourceDescriptor:
        return ResourceDescriptor(
            clazz=ResourceName.ChatModel.OLLAMA_SETUP, connection="ollama", model=MODEL,
            temperature=0.0, num_ctx=8192, think=THINK, keep_alive="30m",
        )

    @skills
    @staticmethod
    def runbooks() -> Skills:
        return Skills.from_local_dir(str(SKILLS_DIR))

    @action(EventType.InputEvent)
    @staticmethod
    def decide(event: Event, ctx: RunnerContext) -> None:
        case = InputEvent.from_event(event).input
        ctx.sensory_memory.set("case", case)
        ctx.sensory_memory.set("t0", time.time())
        system = SYSTEM_SKILL if case["variant"] == "skill" else SYSTEM_INLINE
        user = (
            f"Job: ClickstreamEnrichment\nMemory: {json.dumps(case['memory'])}\n"
            f"Latest observation: {json.dumps(case['observation'])}"
        )
        ctx.send_event(
            ChatRequestEvent(
                model="with_skill" if case["variant"] == "skill" else "inline",
                messages=[ChatMessage.of(MessageRole.SYSTEM, system), ChatMessage.of(MessageRole.USER, user)],
                output_schema=OutputSchema(output_schema=Decision) if SCHEMA == "native" else None,
            )
        )

    @action(EventType.ChatResponseEvent)
    @staticmethod
    def record(event: Event, ctx: RunnerContext) -> None:
        resp = ChatResponseEvent.from_event(event)
        case = ctx.sensory_memory.get("case")
        row = {
            "variant": case["variant"] + ("+think" if THINK else ""), "rep": case["rep"], "scenario": case["name"],
            "expected": case["expected"], "latency_s": round(time.time() - ctx.sensory_memory.get("t0"), 1),
            "retries": resp.retry_count, "ok": resp.is_success, "schema": SCHEMA,
        }
        decision = None
        if resp.is_success:
            try:
                decision = parse_decision(resp.response)
            except ValueError as e:
                row.update(error=f"parse: {e}"[:300], correct=False, raw=resp.response.text[:300])
        else:
            row.update(error=resp.error[:300], correct=False)
        if decision is not None:
            row.update(action=decision.action, interval_ms=decision.interval_ms,
                       step=decision.runbook_step, rationale=decision.rationale)
            row["correct"] = decision.action == case["expected"]
        with open(OUT, "a") as f:
            f.write(json.dumps(row) + "\n")


def main():
    variants = ["skill", "inline"] if len(sys.argv) < 2 or sys.argv[1] == "both" else [sys.argv[1]]
    reps = int(sys.argv[2]) if len(sys.argv) > 2 else 1
    cases = [
        {"variant": v, "rep": r, "name": n, "expected": e, "memory": m, "observation": o}
        for v in variants for r in range(reps) for (n, e, m, o) in SCENARIOS
    ]
    config = Configuration()
    config.set_string("python.pythonpath", f"{sysconfig.get_paths()['purelib']}:{Path(__file__).resolve().parent}")
    config.set_string("python.executable", sys.executable)
    env = StreamExecutionEnvironment.get_execution_environment(config)
    env.set_parallelism(1)
    agents_env = AgentsExecutionEnvironment.get_execution_environment(env=env)
    agents_env.get_config().set(AgentExecutionOptions.MAX_RETRIES, 2)
    # Serialize LLM calls so latency_s is per call, not time queued in Ollama.
    agents_env.get_config().set(AgentExecutionOptions.CHAT_ASYNC, False)
    agents_env.get_config().set(AgentConfigOptions.BASE_LOG_DIR, EVENT_LOG_DIR)
    (
        agents_env.from_datastream(
            input=env.from_collection(cases), key_selector=lambda c: f"{c['variant']}-{c['rep']}-{c['name']}"
        )
        .apply(SpikeLlmAgent())
        .to_datastream()
        .print()
    )
    agents_env.execute()


if __name__ == "__main__":
    sys.exit("Run via: cd spikes && python -c 'import llm_spike; llm_spike.main()' [args] "
             "(the Python workers import the agent class by module name, not from __main__)")
