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
"""Plain Flink stages in front of the agent: window the metric samples per job, then
forward only windows worth the agent's attention."""
from pyflink.common.typeinfo import Types
from pyflink.datastream.functions import KeyedProcessFunction, ProcessWindowFunction
from pyflink.datastream.state import ValueStateDescriptor

# A window counts as skewed when task counts differ by 2+, the hottest TaskManager
# is near saturation while another is well below, and upstream is backpressured.
MIN_TASK_SKEW = 2
HOT_CPU = 0.85
COLD_CPU = 0.6
BACKPRESSURE_MS = 300
# After a config change, forward this many windows so the agent can verify it.
FOLLOWUP_WINDOWS = 6


class SummarizeWindow(ProcessWindowFunction):
    """Samples of one job in one window -> placement, CPU, backpressure, throughput."""

    def process(self, key, context, elements):
        samples = sorted(elements, key=lambda s: s["ts"])
        live = [s for s in samples if s.get("running")]
        configs = [s["config"] for s in samples if s.get("config")]
        summary = {
            "job": key,
            "simulated": bool(samples[-1].get("simulated")),
            "window_end": context.window().end / 1000.0,
            "samples": len(samples),
            "running": bool(live) and len(live) == len(samples),
            "config": configs[-1] if configs else None,
            "recent_failovers": max(s.get("recent_failovers", 0) for s in samples),
            "anomalous": False,
        }
        if live:
            latest = live[-1]
            tasks = latest["tasks_per_tm"]
            cpu = {tm: round(sum(s["cpu"].get(tm, 0) for s in live) / len(live), 2) for tm in tasks}
            counts = list(tasks.values())
            pressured = sum(1 for s in live if s.get("backpressure_ms", 0) >= BACKPRESSURE_MS)
            # Median of per-poll counter deltas: robust to one partial metric read.
            same_job = [s for s in live if s.get("job_id") == latest.get("job_id")]
            deltas = sorted(
                (b["records_in"] - a["records_in"]) / (b["ts"] - a["ts"])
                for a, b in zip(same_job, same_job[1:])
                if b["ts"] - a["ts"] >= 1
            )
            rate = round(deltas[len(deltas) // 2]) if deltas else None
            summary.update(
                tasks_per_tm=tasks,
                cpu=cpu,
                task_skew=max(counts) - min(counts),
                backpressured=pressured * 2 >= len(live),
                records_per_sec=rate,
            )
            summary["anomalous"] = (
                summary["task_skew"] >= MIN_TASK_SKEW
                and max(cpu.values()) >= HOT_CPU
                and min(cpu.values()) <= COLD_CPU
                and summary["backpressured"]
            )
        yield summary


class PersistenceFilter(KeyedProcessFunction):
    """Drops healthy windows. Forwards skewed windows (with how many in a row), the first
    healthy window after them, and the windows right after a config change."""

    def open(self, runtime_context):
        self.state = runtime_context.get_state(
            ValueStateDescriptor("persistence", Types.PICKLED_BYTE_ARRAY())
        )

    def process_element(self, window, ctx):
        st = self.state.value() or {"streak": 0, "config": None, "followups": 0, "prev_skewed": False}
        st["streak"] = st["streak"] + 1 if window["anomalous"] else 0
        config = window.get("config")
        if config and st["config"] and config != st["config"]:
            st["followups"] = FOLLOWUP_WINDOWS
        if config:
            st["config"] = config
        forward = window["anomalous"] or st["prev_skewed"] or st["followups"] > 0
        st["followups"] = max(0, st["followups"] - 1)
        st["prev_skewed"] = window["anomalous"]
        self.state.update(st)
        if forward:
            window["skewed_windows"] = st["streak"]
            yield window
