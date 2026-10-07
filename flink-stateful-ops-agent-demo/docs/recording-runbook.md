# Recording runbook

How to record the muted demo video that `demo-script.md` narrates. Every command and
timing below comes from verified rehearsal runs on an M3 Pro laptop with `TM_CPUS=0.5`
(the default). Run all commands from `flink-agents-demos/flink-stateful-ops-agent-demo`.

## What you record
| Footage | Covers script scenes | Stack needed | Real length |
|---|---|---|---|
| Take A: hero fix, then fleet escalation | 2, 3, 4, 5 | yes | ~7 min |
| Take B: kill the agent mid-redeploy | 6 | yes | ~3 min |
| Screen shots | 1 (docs page), 2 (diagram), 7 (summary card) | no | ~1 min |

Edit these into the 7:00 video: time-lapse the waits, add the chapter cards and hold frames.

## 0. Before the session (once)
Start **Docker Desktop** and the **Ollama** app first.
```bash
bin/demo.sh setup      # only on a new machine: Flink, venv, flink-agents main + #1161, jar, model
bin/demo.sh up         # Kafka, target cluster (skewed), agent cluster, Ollama warm, platform-sim
bin/demo.sh status     # everything "up", target "skewed"
```
`up` takes about 1–2 minutes. The laptop load stays moderate: the three target TaskManagers
are capped at 0.5 CPU each.

## 1. Screen setup
Use a 1920×1080 recording area: macOS **Cmd+Shift+5** → *Record Selected Portion*, or set the
display to 1920×1080. Turn on Do Not Disturb. Quit Slack and mail. Hide the Chrome bookmarks bar.

Open these before you start recording:

| Where | What | Used in |
|---|---|---|
| Chrome tab 1 | Flink docs: https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/deployment/tasks-scheduling/balanced_tasks_scheduling/ | scene 1 |
| Chrome tab 2 | **Dashboard** http://localhost:8090 (keys `1` = real job, `2` = fleet) | scenes 3–6 |
| Chrome tab 3 | **Agent Flink UI** http://localhost:8082 → Running Jobs → `stateful-ops-agent` | scenes 2, 6 |
| Chrome tab 4 | **Target Flink UI** http://localhost:8081 → `ClickstreamEnrichment` | scenes 3, 4 |
| VS Code (optional cutaways) | `skills/balanced-task-scheduling/SKILL.md` (decision table, line 57) · `agent-job/ops_agent.py` (`durable_execute_async`, line 245) · `agent-job/ops_runbook.py` (`allowed_actions`, line 106) | scenes 1, 4, 5 |
| iTerm, font ≥ 20pt | the commands below | all takes |

The dashboard fits 1080p exactly. Chrome zoom 100%. Use full screen (Cmd+Ctrl+F) on the dashboard tab.

**Target UI (8081), before vs after.** The click source emits a fixed 9,500 events/s
(`TARGET_RATE`, default 19000 × `TM_CPUS`). The skewed placement can't keep up; the balanced one can.
- Before (NONE, 4/3/2): `SessionScore -> Sink` is red (the hot subtask is 100% busy), `Source: Clicks -> Enrich`
  is backpressured (300–600 ms/s), and the job does ~6.5–7.5k records/s.
- After (TASKS, 3/3/3): backpressure 0 and 9.5k records/s. SessionScore busy falls to ~50% about 1 min after
  the redeploy (JIT warm-up), then to ~25–40%. Take the "after" shot at or after KEEP.

## 2. Take A: scenes 2–5
**Reset, then start recording.**
```bash
bin/demo.sh reset      # ~1.5–2 min. Ends with "Ready for a take".
```
`reset` cancels the agent, clears the dashboard, recreates the target cluster until NONE mode
places tasks unevenly (it retries automatically), restarts the agent cluster, warms the model, and
waits until the target falls behind its input (Enrich backpressure ≥ 300 ms/s).

T = seconds after you run `bin/demo.sh agent`. Measured times; allow ±10 s.

| T | Do / run | Look at | What it shows (script scene) |
|---|---|---|---|
| 0 | `bin/demo.sh agent` | terminal | "Job has been submitted" |
| 5–15 | — | **Agent UI** (8082) → `stateful-ops-agent` → job graph | Kafka source → `window 15s` → `persistence filter` → `OpsAgent` at parallelism 4 (scene 2) |
| 15–25 | — | **Target UI** (8081) → ClickstreamEnrichment job graph; click `Source: Clicks -> Enrich` → *BackPressure* tab | SessionScore red, Enrich backpressured (scene 3) |
| 25 | press `1` | **Dashboard** | red bar: the TaskManager with 4 tasks at ~100% CPU, another with 2 (scene 3) |
| ~18, ~34 | — | Dashboard timeline | `skewed 1/3 windows: WAIT`, `skewed 2/3 windows: WAIT` (scene 3, "it waits") |
| ~52 | — | Dashboard: *Latest decision* flashes, *Agent memory* | `APPLY_TASKS`, `source: LLM (qwen3:8b, ~5–7 s)`; memory shows incident #1 and the **baseline** (~7.5k rec/s) (scene 4) |
| 52–77 | — | Dashboard: *Deployment API* card ticks `stop-with-savepoint → recreate cluster → submit from savepoint → wait for RUNNING`; TM bars show "cluster restarting…" | the real redeploy, ~25 s (scene 4). Optional: Target UI shows the job coming back |
| ~77 | — | Dashboard TM bars | **3 / 3 / 3**, all green. "redeploys executed by the platform: 1" |
| 77–127 | — | timeline countdown `cooldown: verify in …s`, records/s rising to 9.5k | the agent waits out the cooldown before judging |
| ~127 | **hold 3 s** | *Latest decision* | `KEEP`, throughput ≥ baseline (~7.5k → 9.5k rec/s, the full input rate); memory: `closed: #1 KEPT` (scene 4) |
| 130–140 | — | **Target UI** (8081) job graph | after shot: no backpressure, SessionScore no longer red (scene 4, optional) |
| ~140 | `bin/demo.sh fleet-incident`, then press `2` | **Dashboard tab 2** | SIMULATED FLEET label; `payments-sessionizer` tile turns red, TASKS already on, 2 failovers (scene 5) |

Fleet timings, F = seconds after `fleet-incident`:

| F | Look at | What it shows |
|---|---|---|
| ~25, ~40 | timeline | `skewed 1/3`, `skewed 2/3`: WAIT |
| ~60 | *Latest decision*, memory | `INCREASE_INTERVAL` (LLM) → chip `slot.request.max-interval: 70ms`; memory: `increases 1 of 3` |
| ~120 | same | `INCREASE_INTERVAL` → 120 ms; attempts table grows, each row with "after" measurements |
| ~180 | same | `INCREASE_INTERVAL` → 170 ms |
| ~236 | **hold 3 s** on *Escalation filed* + *Latest decision* | `ESCALATE` from `guardrail (code, no LLM)`; the FLINK-38715 report with the attempts table, written from memory |
| ~250+ | memory note, timeline | `escalated as …: hands off` — the agent keeps watching but changes nothing |

Good cutaways during the cooldowns (30–45 s of nothing new each): the SKILL.md decision table, and
`allowed_actions` in `ops_runbook.py` ("the limit is code, not a prompt").

**Stop recording.** Header counters at the end of a take: ~3,500 metric samples → 26–29 windows
→ 5 LLM calls → 5 side effects; KEPT 1 · REVERTED 0 · ESCALATED 1.

## 3. Take B: scene 6 (kill the agent)
Put the terminal and the **Agent UI** job page side by side, with the dashboard (tab 1) one swipe away.
```bash
bin/demo.sh reset
# start recording
bin/demo.sh agent
bin/demo.sh kill-agent-tm --wait
```
`--wait` blocks until the agent starts the redeploy (T ≈ 50 s). Five seconds later it prints and
runs, for example:
```
Redeploy in flight: ClickstreamEnrichment#1.1 (ClickstreamEnrichment), sent by the agent TaskManager pid 41383
$ kill -9 41383
```

| After the kill | Look at | What it shows |
|---|---|---|
| 0–15 s | Agent UI | job RESTARTING → RUNNING on the surviving TaskManager; dashboard header `restarts 1` |
| ~25–30 s | Dashboard *Deployment API* card | steps finish (rehearsal: 25.0 s, marked `reconciled`); green note: *agent recovered: its reconciler found this request and waited for it instead of sending it again* |
| — | Dashboard | **redeploys executed by the platform: 1** (hold here). The platform does not de-duplicate, so a resend would show 2 |
| — | Agent memory | baseline and attempt survived; the attempt is marked `reconciled` |
| ~T+127 | Latest decision | KEEP, as in take A (rehearsal: T+127, baseline 7.6k → 9.5k rec/s) |

Header counters at the end of take B: `restarts 1`, KEPT 1 · REVERTED 0 · ESCALATED 0.

## 4. Screen-only shots
- Scene 1: Chrome tab 1. Select each quoted phrase with the mouse as you scroll; the selection is the highlight.
- Scene 2: the architecture diagram from the deck (5 s).
- Scene 7: a title card in the editor: "Waited. Experimented. Remembered. Stopped." KEPT 1 · REVERTED 0 · ESCALATED 1.

## 5. Editing to 7:00
Map: scene 2 ← take A T 0–25 · scene 3 ← T 25–52 · scene 4 ← T 52–140 · scene 5 ← fleet F 0–250 ·
scene 6 ← take B. Time-lapse the countdowns ×4–×8 with a "×N" badge, add 2 s chapter cards and
2–3 s holds on: bars at 3/3/3, KEEP, ESCALATE, "redeploys: 1". iMovie or ScreenFlow is enough.

## Retakes and troubleshooting
- Anything off: `bin/demo.sh reset` and go again. Each take starts clean.
- Between takes use `reset`, not `up`. `up` re-skews the target but leaves an old agent job running, which would start acting on it.
- `Target placement did not skew after 3 attempts`: run `bin/demo.sh reset` again.
- `An agent job is already running`: `bin/demo.sh reset`.
- Dashboard says agent `NO JOB`: `bin/demo.sh status`; logs in `tmp/platform/platform.log` and `flink-2.2.1/log/`.
- First LLM call slow (> 20 s): the model was unloaded; `reset` warms it, or run `bin/demo.sh up`.
- `reset` warns `target not backpressured yet`: the skew windows start later than the table says; wait ~30 s before `agent`, or `reset` again.
- Target still red after the fix (or never backpressured before it): your CPU differs from the rehearsal machine. Set `TARGET_RATE` (lower if red after, higher if not backpressured before) and `reset`.
- After take B the agent cluster has one TaskManager left; `reset` restores two.

## Shut down
```bash
bin/demo.sh down         # clusters + platform-sim (Kafka and the model stay)
bin/demo.sh down --all   # also Kafka, and unload the model
```
