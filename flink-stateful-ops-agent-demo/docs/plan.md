# Plan: new demo module `flink-stateful-ops-agent-demo` (re:Invent OPN303, slides 10–12)

## Decisions (2026-10-02)
- Direction: build a new stateful tuning demo around the balanced-scheduling runbook. The original `flink-operations-agent-demo` stays untouched.
- Demo format (2026-10-03): a **pre-recorded video** (7:00, slide 11), not a live demo. No venue network, latency or live-failure risk. Real waits (30s windows, redeploys, the ~65s meter lag) are time-lapsed in editing with an on-screen wall clock. Presenter narrates live over the muted video from `docs/demo-script.md`, so it can be rehearsed.
- LLM (2026-10-03): record with **Ollama qwen3:8b**. It is verified 21/21 and needs no account, so anyone can reproduce the video from the public repo. Bedrock is an optional upgrade if Mayank (AWS) provides an account, and it doesn't block anything.
- Target cluster: Docker, with a JobManager and 3 TaskManagers, each TaskManager limited to one CPU.
- Current scope: P0 test, then the P1 skeleton, then check in.
- flink-agents version: a local build of main + PR #1161, until a release contains the fix. Without it there's no "no double redeploy" beat (see P0 findings).
- The hero job's live fix is NONE → TASKS on unbounded input. The "+50 ms" step stays on the simulated fleet job, because post-failover imbalance didn't reproduce on the hero job (see P0 findings: skew).

## Demo story
1. **The page.** ClickstreamEnrichment is backpressured. "An autoscaler would buy more machines."
2. **The agent waits and remembers.**
   - Per-job keyed state collects evidence across windows: one TaskManager is at 100% CPU with 4 tasks, while another has 2.
   - It loads the runbook skill, records a baseline, and applies `TASKS` scheduling. That side effect runs exactly once.
   - It verifies against the baseline: balanced, throughput up, so KEEP. No new machines.
3. **When the fix doesn't hold** (simulated fleet job): +50 ms, then +50 ms, then it stops and escalates with a report built from its own memory.
4. **Chaos.** Kill the agent mid-action. It recovers from the checkpoint, doesn't redeploy twice, and still remembers.
5. **Close:** "It didn't just answer. It remembered, experimented, and knew when to stop." Then on to slide 12.

## Q&A
**Is the existing ops agent stateless?** Its reasoning is. It runs on a stateful engine but uses Flink only as a runner:
- Only sensory memory, which is wiped after every inspection, so each check starts from zero.
- Every job shares one key (`lambda x: 1`): no per-job memory, and inspections run one at a time.
- Its only "history" is watermark JSON files written by an outside script, which aren't checkpointed.
- No checkpoints or restart strategy: one LLM timeout killed the job.

**How is this different from auto-sizing?**
- An autoscaler is a fixed formula on one metric and one setting (parallelism), and it already exists in the Flink K8s Operator.
- For skew it does the wrong thing: it adds resources, when the real problem is placement. The fix is the same resources, placed better.
- The stateful agent follows a written runbook across several settings (scheduler mode, `slot.request.max-interval`, then a human).
- It runs experiments: baseline → change → verify → keep or revert → escalate with evidence.
- Each decision depends on what it has already tried. That is episodic memory, not a window of metrics.

## Why a new demo (the original's weaknesses)
- The original is basically auto-sizing. The Flink K8s Operator autoscaler already does that without an LLM, so expect the question "why not the autoscaler?"
- It doesn't show anything stateful: one shared key, only sensory memory, history kept in files, no checkpoints or durability, and a cron-like trigger.

## New demo case: "The runbook is stateful" (Flink 2.2 Balanced Tasks Scheduling, FLIP-370)
Every line of the Flink doc's runbook depends on what happened before:
1. "if you are seeing these bottlenecks": the bottleneck must persist across windows; a single snapshot isn't enough.
2. "should not use it if not seeing bottlenecks, may degrade": compare against a baseline and revert if worse.
3. "increase slot.request.max-interval by 50 ms each time": the agent needs the current value and the number of attempts.
4. "if still unbalanced after sufficient increases, report in FLINK-38715": escalate with the full history as evidence.

A stateless, request-driven agent needs an external DB, scheduler, queue and partitioner to follow this runbook. In Flink Agents it is per-job keyed state, checkpointed together with the stream.

## Verified facts (2026-10-01)
- Latest flink-agents release: 0.3.1 (PyPI). It ships dist jars for Flink 1.20, 2.0, 2.1, 2.2 and 2.3.
- `taskmanager.load-balance.mode: TASKS` exists only in Flink ≥ 2.2. Flink 1.20 has only NONE/SLOTS.
- `slot.request.max-interval` default is 20 ms (2.2).
- In a Flink 2.2 session cluster, both options are read from the **cluster** config. `JobMasterServiceLeadershipRunnerFactory` passes the cluster `configuration` into `DefaultSlotPoolServiceSchedulerFactory.fromConfiguration`, so a job-level `-D` is ignored. The remedy is to redeploy the target cluster from a savepoint (in production, a FlinkDeployment spec change).
- Stack: Flink 2.2.1, apache-flink 2.2.1, flink-connector-kafka 5.0.0-2.2, flink-agents 0.4-SNAPSHOT built locally from main + PR #1161 (0.4 is not released; see P0 findings: durability).
- Features to use (all are in 0.3.1; the durability fixes are not):
  - short-term memory plus TTL (`short-term-memory.state-ttl.ms`)
  - action state store (Kafka) plus `durable_execute`
  - Skills (`@skills`, `Skills.from_local_dir`, `load_skill`)
  - event log (SLF4J, shown in the Flink UI Logs tab, or file)
  - built-in metrics
  - `output_schema`
  - async/parallel LLM
  - MCP
  - Bedrock chat model (Java, used cross-language; optional, needs AWS credentials)

## Architecture (a single Flink job; stream processing before reasoning)
```
collector (REST poll, 5s) ─┐
fleet simulator (N jobs) ──┴─> Kafka flink_metrics
  -> keyBy(job_name) -> 30s windows -> features (TM task skew, hot-TM busy%, backpressure, throughput)
  -> KeyedProcessFunction: emit only if the anomaly persists ≥3 windows, or for follow-up windows after an incident
  -> Flink Agents agent (keyed by job_name, parallelism 4)
       memory: baseline, experiments[], current_config, phase, cooldown
       skill:  balanced-task-scheduling, loaded deterministically into every decision prompt
       tools:  redeploy_with_config (side effect, durable), get_tm_load
       guardrails in code: cooldown, max attempts, then escalate
  -> Kafka operations_record -> audit console (normal / auto-remediated / manual)
```
- Agent cluster: local Flink 2.2.1 standalone. It is never restarted by remediation.
- Target cluster: docker compose with a JobManager and 3 TaskManagers (2 slots each, `cpus: 1`). The hero job has vertex A at p=6 and B at p=3, both CPU-heavy. The default strategy gives a 4/3/2 task skew, so the hot TaskManager backpressures.
- Durability: checkpoint every 10s, fixed-delay restart, Kafka action state store.

## Phases (each ends with something that can be demoed)

**Status (2026-10-03):** P0–P4 done and verified end to end on the laptop (`TM_CPUS=0.5`): hero fix with a real redeploy and KEEP, fleet escalation with the hands-off hold, and the agent TaskManager kill mid-redeploy (restarts 1, redeploys 1, reconciled). The target job's input is rate-limited to `TARGET_RATE` (19000 × `TM_CPUS` = 9,500 events/s): the skewed placement falls behind (~6.5–7.5k rec/s, backpressured, hot SessionScore 100% busy), the balanced one keeps up (9.5k, no backpressure, ~25–40% busy), so the Flink UI shows the fix too. P5 is next: record with `docs/recording-runbook.md`.

- P0 spike, about 1 day. Verify on 2.2.1 + 0.3.1 Python:
  - short-term memory persists across runs and checkpoints
  - after a TaskManager kill, durable_execute does not replay the side effect
  - Skills load
  - event log shows in the UI
  - structured output is reliable (Bedrock vs Ollama)
  - the skew reproduces and TASKS mode fixes it
- P1: new module skeleton on Flink 2.2 + main/#1161:
  - key by job_name, parallelism 4
  - checkpoints, restart strategy, Kafka action store
  - memory: baseline, actions, config, cooldown, windows with skew
  - the runbook loaded deterministically; `output_schema` decisions
  - tools over `target_ctl`: redeploy (`durable_execute`), get_tm_load
  - manual trigger, start/stop scripts, `setup_flink.sh` for the local build
- P2: stream-first pipeline and fleet simulator (scale: N jobs, 4 subtasks, events→incidents→LLM-call counters).
- P3: hero scenario:
  - SOP as a Skill
  - memory-driven loop: observe → baseline → apply TASKS → verify → keep/revert → +50 ms → escalate
  - real target-cluster redeploy
- P4: demo UX:
  - per-job memory timeline view
  - per-TaskManager CPU bars
  - scene trigger scripts
  - event log
- P5: record and edit the demo video (the primary demo, slide 11). Shoot to `docs/demo-script.md`: its "On screen" lines are the shot list, with chapter cards and hold frames for live narration. Several takes per scene, time-lapse the waits, then a final cut. Hidden slide 18 is redundant.

## Demo video flow (7:00, muted, narrated live; full script in `docs/demo-script.md`)
Talk clock: slide 10 setup 8:00–8:30, video 8:30–15:30, 0:30 slack, slide 12 at 16:00.

| Video time | Beat |
|---|---|
| 0:00 | The Flink doc runbook, highlighting the four stateful phrases |
| 0:45 | Architecture plus Flink UI: agent operator at p=4; N jobs streaming; counters for events, incidents and LLM calls |
| 1:30 | Hero job skewed (CPU bars). The agent waits 3 windows instead of reacting to a blip, and memory shows the baseline |
| 2:45 | Agent applies TASKS (durable side effect), the target redeploys from a savepoint, and the bars even out. Verified against the baseline, so KEEP |
| 4:00 | Fleet job (simulated) where the fix doesn't hold: +50 ms per try until the limit, then escalate with a report built from memory |
| 5:15 | Kill the agent's TaskManager mid-action: it recovers from the checkpoint, does not redeploy twice, and memory is intact |
| 6:30 | Summary card, then click to slide 12 |

Say it rather than show it:
- thousands of jobs through key partitioning
- exactly-once action consistency needs the same per-key order after recovery
- memory TTL
- in production this is a K8s operator spec change, gated by approvals and blast-radius limits

Recording rules:
- Real runs only. Speed up waits but never fake a result, and label the simulated fleet job on screen.
- Show a wall clock (or a "×N" badge) on every time-lapsed segment, so the 3-window wait stays honest.
- Record at 1080p+ with large terminal and UI fonts; the video plays on a big screen.

## Risks
- ~~Whether the skew is deterministic on docker TaskManagers~~ Resolved: NONE skewed 5/5 times, TASKS balanced 4/4 (see P0 findings: skew).
- Post-failover imbalance is not reproducible on demand: 0 of 3 TaskManager kills under TASKS. So the "+50 ms" chapter uses the simulated fleet and must be labeled honestly.
- The Flink throughput meter lags about 65s behind a redeploy. Verify with `target_ctl.measure_throughput` (a 20s counter window) instead.
- ~~Bedrock needs AWS credentials~~ Not blocking: the video is recorded with Ollama qwen3:8b (21/21, ~3.4s per decision). Bedrock is optional.
- The recording and editing time must fit before the deck deadline; leave room for retakes.
- **flink-agents version:** durability needs fixes that aren't in 0.3.1 (see P0 findings).

## P0 findings (2026-10-02): memory and durable execution
The spike lives in `spikes/`. The test kills the agent TaskManager with `kill -9` while actions are in flight.

What we verified:
- Short-term memory survives a TaskManager kill on every version: after restore, `runs` and `history` continue where they left off.
- Per-key concurrency only holds with `async def` actions plus `await ctx.durable_execute_async`. A synchronous action blocks the subtask, so keys on that subtask run one after another.

Durability results by version:

| Version | Kill during APPLY, with reconciler | Kill during APPLY, no reconciler | Kill during OBSERVE (APPLY already done) |
|---|---|---|---|
| 0.3.1 | ❌ re-applies; restores 0 states | duplicate | ❌ re-applies |
| main 99103da6 | ❌ only correct when no checkpoint lands mid-action | duplicate | ❌ same as left |
| main + PR #1161 (14ee1feb) | ✅ the reconciler finds the receipt and skips | duplicate (expected baseline) | ✅ APPLY result replayed from the store; only OBSERVE re-runs |

Upstream status:
- **#1095 (merged):** cold-start topic-creation timeout.
- **#1101 (merged):** the restore stopped at the first empty poll.
- **#1158 (open):** the recovery marker is the end offset at snapshot time. Python async actions don't block the checkpoint barrier, so durable records written before a checkpoint by still-running actions get skipped. **PR #1161** fixes it and is verified end to end above: the restore now seeks to each in-flight key's latest record.
- **#1174 (open):** the Fluss variant of #1158.

**Decided:** pin the demo to main + #1161, then the first release that contains it. 0.3.1 can't show "no double redeploy".

### Gotchas
1. Don't copy flink-agents dist jars into `$FLINK_HOME/lib` for Python jobs. The duplicate kafka-clients cause a `JmxReporter` ClassCastException.
2. Set `python.executable`, and export `PYTHONPATH=<venv purelib>` before starting TaskManagers (pemja).
3. The agent class must live in an importable module. Submit with `-pym <main> -pyfs <absolute dir>`; a relative `-pyfs .` failed with ModuleNotFoundError.
4. Add `pemja` to `classloader.parent-first-patterns.additional`. Otherwise every restart fails with "Native Library already loaded".
5. The parent-first side effect: a TaskManager's Python module cache outlives the job. **Restart the cluster after changing agent code**, or the old code runs silently.
6. The agent input must be Python objects. Put a `.map(lambda s: s)` after the Java KafkaSource.
7. Read the input with `InputEvent.from_event(event).input`.
8. Building flink-agents from source:
   - compile on JDK 17 (output stays Java 11 bytecode)
   - install the Python package with `FLINK_AGENTS_SKIP_JAR_DOWNLOAD=1`
   - **use `mvn clean install` for the `dist/*` modules.** Without `clean`, maven-shade re-shades the previous jar and the stale runtime classes win.

## P0 findings (2026-10-02): skew and remediation
Setup:
- Target cluster: `target-cluster/` (docker, 3 TaskManagers × 2 slots, `cpus: 1` each).
- Hero job: `target-jobs/clickstream-enrichment` (Enrich p=6 → keyBy → SessionScore p=3, both CPU-heavy, unbounded datagen).
- Control: `target-cluster/target_ctl.py`.
- Trial runners: `spikes/skew_trials.py` and `spikes/failover_trials.py`.

| Mode | Trials | Tasks per TaskManager | CPU per TaskManager | Throughput (SessionScore in) |
|---|---|---|---|---|
| NONE (default) | 5/5 skewed | 4/3/2 or 4/2/3 (the hot TaskManager varies) | 100% / ~68% / ~27% | ~17k rec/s; every Enrich subtask 70–90% backpressured |
| TASKS | 4/4 balanced | 3/3/3 | 100% / 100% / 100% | ~27–28k rec/s (**+60% on the same 3 CPUs**) |
| TASKS, after killing a TaskManager | 3/3 balanced | 3/3/3 | — | job recovered in 16–28s |

- **Remediation:** redeploy the cluster from a savepoint, which takes about 13–16s (`target_ctl.redeploy`). Both options are cluster-level in a session cluster.
- **Metrics:**
  - CPU and busy-time metrics saturate about 16s after a redeploy.
  - `numRecordsInPerSecond` is a 60s meter, so it lags about 65s.
  - The JobManager's REST metric store answers from its *previous* fetch, so call `refresh_metrics()` before reading.
  - `measure_throughput(jid, 20)` agrees with the meter to within 2%.
- **Runbook source:**
  - The Flink **2.2** docs only describe the mechanism, plus "don't use without bottlenecks".
  - The failover note ("increase `slot.request.max-interval` by 50 ms each time … report in FLINK-38715") is in the **2.3/master** docs.
  - The note comes from FLINK-38687 release testing. There the imbalance was random and needed several slot-sharing groups plus spare TaskManagers.
  - The skill should cite both docs pages.
- **Why not the autoscaler:** the backpressure is real, so an autoscaler would add capacity. But the fix is placement, on the same machines.

## P0 findings (2026-10-02): LLM decisions
Spike: `spikes/llm_spike.py` covers 7 runbook scenarios (WAIT, APPLY_TASKS, KEEP, REVERT, INCREASE_INTERVAL, ESCALATE, NO_ACTION). It runs through a flink-agents agent on a PyFlink mini-cluster with Ollama qwen3:8b at temperature 0.

| Variant | Correct | Latency per call |
|---|---|---|
| **Runbook in the system prompt, `output_schema`, no thinking** | **21/21** (3 reps), 0 parse errors, 0 retries | median 3.4s, max 7.1s |
| Same, thinking on | 7/7 | 13–60s (no accuracy gain; slows every take) |
| Runbook as a Skill (model must call `load_skill`), with or without `output_schema`, with or without thinking | 2–3/7: never called `load_skill` | 2–40s |

- The runbook needs an explicit **decision table** (situation → action) plus a `runbook_step` field placed before `action` in the output. With prose alone, the model jumps to APPLY_TASKS (4/7).
- Ollama native structured output (`format`) constrains decoding to the JSON schema, so the model **cannot emit a tool call** while it's on. Even without it, qwen3:8b skips `load_skill` whenever the prompt also asks for JSON (0/6 in direct replays).
- **Decision:** the runbook is still a standard Agent Skill (`skills/balanced-task-scheduling/SKILL.md`, citing the Flink docs), but the agent **loads it deterministically** into every decision prompt. The talking point: "an ops agent shouldn't get to choose whether to read the runbook." Re-test model-driven `load_skill` on Bedrock once credentials exist.
- Python agent jobs: the agent class must be imported from a module, not `__main__`. For local runs use `python -c 'import mod; mod.main()'`, and set `python.executable` to the venv interpreter.
- Event log: `baseLogDir` → `FileEventLogger` writes per-subtask JSONL. Strings are truncated at 2000 chars (`event-log.standard.max-string-length`).

## Local dev safety (the presenter's MacBook: M3 Pro, 12 cores, 36 GB)
- Develop with `TM_CPUS=0.5`, so the target cluster is hard-capped at 1.5 cores (~12% of the machine). Use the full `TM_CPUS=1` only for recording takes.
- Ollama runs only during decisions (~3s bursts).
- Run `target_ctl.py down`, stop the agent cluster and stop Kafka whenever a test session ends.
- Check `pmset -g therm` for thermal warnings during long runs.
