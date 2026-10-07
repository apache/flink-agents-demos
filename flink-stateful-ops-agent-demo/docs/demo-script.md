# Demo script: live narration over a muted video (OPN303, slides 10–12)

Status: numbers come from verified rehearsal takes (see `recording-runbook.md`). Re-check them against your final take; throughput varies a little from run to run. The script also serves as the shot list: each scene's **On screen** is what the recording must show.

## Timing budget (from the deck's speaker notes)
| Talk clock | Slide | Who | Length |
|---|---|---|---|
| 8:00 | 10: setup | Weiqing | 0:30 |
| 8:30 | 11: demo video (muted) | Weiqing narrates | 7:00 |
| 15:30 | slack (late start, a pause, a laugh) | | 0:30 |
| 16:00 | 12: Agent ops patterns | Weiqing | 2:00 |

Pace: about 120 words per minute. Each scene's lines are shorter than the scene, so the video never has to wait for you. Lines marked *(optional)* are the first to drop if you fall behind.

Silence is planned. Scenes 3–6 are about half narration: let the audience watch the redeploy, the kill and the hold frames without talking over them.

## How to stay in sync
- Each scene opens with a 2-second chapter card ("1 · The runbook"). It's a resync point: if you're behind, jump to that scene's first line.
- Each key moment gets a 2–3 second hold frame: bars even out, KEEP, ESCALATE, "redeploys: 1". Land the punchline on the hold.
- On-screen highlight boxes do the pointing, so you never need a laser pointer.
- Waits are time-lapsed, with a wall clock or "×N" badge on screen. Say "time-lapsed" once, in scene 3.

---

## Hand-in
Mayank (end of slide 9): *"Weiqing, show them what a Flink job on call looks like."*

## Slide 10: setup (0:30)
**Proposed new slide text.** The current slide describes the old demo: vector store, four sample jobs, DashScope→Bedrock prep.
- Title: Flink watching Flink
- Subtitle: An operations agent, itself a Flink job, follows a stateful runbook for the jobs it watches.
- 01 WAIT: Is it a real bottleneck, or a blip?
- 02 EXPERIMENT: Baseline, change one setting, verify.
- 03 REMEMBER: Every attempt lives in per-job Flink state.
- 04 STOP: Keep, revert, or hand off with evidence.
- Footer: EVERY RUN ENDS ONE WAY: KEPT · REVERTED · ESCALATED TO A PERSON

**Say:**
> Thanks, Mayank. This is a recording, so I'll narrate.
> The agent is a Flink job, watching other Flink jobs. It isn't answering a question. It's following a runbook that takes several tries, so it has to remember what it did.
> Watch for three things: it waits, it experiments, and it knows when to stop.

*(Click: start the video.)*

---

## Scene 1: the runbook (0:00–0:45)
**On screen:** the Flink 2.2 docs, Balanced Tasks Scheduling. Four phrases highlight one at a time, about 8s each.
**Cue:** the first highlight appears.

**Say:**
> This is a real runbook: the Flink 2.2 docs for balanced task scheduling.
> "If you're seeing bottlenecks": that's over time, not one snapshot.
> "It may degrade": so take a baseline, and be ready to revert.
> "Increase by 50 milliseconds each time": so remember what you tried.
> "If it's still unbalanced, report it": with the history as evidence.
> Every step depends on the one before. The runbook is stateful.

## Scene 2: the agent is a Flink job (0:45–1:30)
**On screen:** the architecture diagram (5s), then the Flink UI job graph: Kafka source → windows → persistence filter → agent operator at parallelism 4. Then the dashboard's header counters: metric samples → windows to the agent → LLM calls → side effects (a hold frame from the end of take A works best here).
**Cue:** the job graph appears.

**Say:**
> Here's the agent: one Flink job. Metrics from every watched job stream in from Kafka, keyed by job name.
> Windows and a filter run first, so the model only sees problems that persist. Look at the funnel: by the end of this run, about 3,500 metric samples, fewer than 30 windows reach the agent, and just 5 model calls.
> Then the agent operator, at parallelism four. Each job's memory is Flink keyed state, checkpointed with the stream.

## Scene 3: the page, and the wait (1:30–2:45)
**On screen:** the target cluster's per-TaskManager CPU bars (one at 100% with 4 tasks, one with 2) and backpressure on ClickstreamEnrichment in the Flink UI. Then the agent's memory timeline, time-lapsed: window 1 skewed, window 2 skewed, window 3 skewed, baseline recorded.
**Cue:** the red bar.

**Say:**
> Now the problem. ClickstreamEnrichment is backpressured. One TaskManager is pinned at 100 percent with four tasks; another has two.
> An autoscaler would add machines. But this cluster isn't too small. The work is badly placed.
> The agent doesn't react to one bad window. This part is time-lapsed. One window, two, three: the skew persists.
> Only then does it act, and first it writes a baseline into memory: throughput, and tasks per TaskManager.

## Scene 4: the fix, verified (2:45–4:00)
**On screen:** the dashboard's *Latest decision* card (`APPLY_TASKS`, the runbook step, the rationale, "source: LLM"). Then the *Deployment API* card ticking through stop-with-savepoint → recreate cluster → submit from savepoint → RUNNING (time-lapsed; optional cutaway to `ctx.durable_execute_async` in `agent-job/ops_agent.py`), and the bars evening out to 3/3/3. Then the verify decision, KEEP: baseline ~7.5k records/s → 9.5k, the job's full input rate, with backpressure gone (optional cutaway: the target Flink UI, SessionScore no longer red). **Hold** on KEEP.
**Cue:** the APPLY_TASKS decision appears.

**Say:**
> The decision comes back structured: which runbook step, which action, and why.
> The action: switch the cluster to TASKS load balancing. That's a redeploy, a real side effect, so it runs as a durable action, with a stable request ID and its result checkpointed with the agent's state.
> The cluster restarts from a savepoint, and... three, three, three.
> The agent checks against its own baseline: balanced, backpressure gone, and the job now keeps up with its full input. Decision: keep.
> Same three machines. No new hardware.

*(Rehearsals measured a baseline of 6.4k–7.6k records/s → 9.5k, i.e. +25% to +48%. If your take shows a clear number, say it instead.)*

## Scene 5: when the fix doesn't hold (4:00–5:15)
**On screen:** the label **SIMULATED FLEET** stays visible the whole scene. The `payments-sessionizer` tile turns red: TASKS already on, skew back after 2 failovers. Memory timeline: interval 20 → 70 ms → still skewed → 120 → 170 → ESCALATE ("guardrail (code, no LLM)"). Then the escalation report: a drafted FLINK-38715 comment with an attempts table built from memory. **Hold** on ESCALATE.
**Cue:** the SIMULATED label.

**Say:**
> Not every fix holds. This one is a simulated fleet job. TASKS is already on, but after two failovers the skew is back.
> The runbook says: raise the slot request interval by 50 milliseconds and check again. The agent knows the current value and how many times it has tried, because that's in its memory.
> Seventy. Still skewed. One-twenty. Still skewed. One-seventy.
> Then it stops. That limit is code, not a prompt.
> It escalates, and the report writes itself from memory: every attempt, every measurement, ready for a human.
> *(optional)* And from then on it's hands-off: it keeps watching, but changes nothing until a person steps in.

## Scene 6: break the agent (5:15–6:30)
**On screen:** a terminal running `kill -9` on the agent's TaskManager while a redeploy is in flight. The Flink UI goes RESTARTING → RUNNING from the checkpoint (time-lapsed). The dashboard's Deployment API card shows the green note "agent recovered: its reconciler found this request and waited for it instead of sending it again", and **redeploys executed by the platform: 1**. The memory timeline is intact. **Hold** on "redeploys: 1".
**Cue:** the kill command.

**Say:**
> Now let's break the agent. I kill its TaskManager in the middle of an action, while the redeploy is in flight.
> Flink restarts the job from the last checkpoint. Two questions.
> Does it redeploy the target twice? No. On recovery, the durable action checks the platform for its request ID, finds it already running, and waits for it instead of sending it again. One redeploy.
> Does it forget? No. The baseline and every attempt come back with the checkpoint.
> *(optional)* In a plain process you'd need your own database and an idempotency scheme for this. Here it's the engine.

## Scene 7: close (6:30–7:00)
**On screen:** a summary card: "Waited. Experimented. Remembered. Stopped." with outcome counts KEPT 1 · REVERTED 0 · ESCALATED 1.
**Cue:** the summary card.

**Say:**
> So it didn't just answer. It waited for evidence, ran an experiment, remembered every step, survived a crash, and knew when to hand off to a person.
> No external database, no scheduler, no queue. Just Flink state.

*(Click: slide 12.)*

---

## Bridge into slide 12
Replace the first line of slide 12's notes ("what you just saw is closest to pattern one") with:
> That was one job and one runbook. You just saw all three patterns in miniature: the stream filtered before the model reasoned, each job's context lived in keyed state, and the guardrails were code. Here's how they look at LinkedIn scale.

## Say it, don't show it (slide 12 or Q&A)
- Scale: the agent is keyed by job name, so thousands of jobs spread across subtasks. Each key's actions stay in order, which is also what makes exactly-once actions possible after recovery.
- Memory has a TTL, so a retired job's state expires on its own.
- In production the "redeploy" is a Kubernetes operator spec change, gated by approvals and blast-radius limits.
- The model is a local open model (Ollama qwen3:8b); any supported provider, Bedrock included, is a config change.

## Q&A prep
- **Why not the autoscaler?** It's a fixed formula on one metric and one setting, parallelism. For skew it adds machines when the problem is placement. The agent follows a written runbook across several settings, runs an experiment, and escalates with evidence.
- **Why not a regular agent framework?** It's request-driven: you'd add a database for memory, a scheduler to wait across windows, a queue and partitioner for many jobs, and idempotency for side effects. Here those are keyed state, windows, key partitioning, and durable execution in one job.
- **Is the fleet job real?** The hero job is real Flink on a real cluster. The fleet job is simulated, because the post-failover imbalance it shows doesn't reproduce on demand. The video labels it.
- **Which version?** flink-agents built from main plus the durable-execution reconciler fix (apache/flink-agents#1161), heading into 0.4, on Flink 2.2.

## Deck changes this script needs
- Slide 10: replace the text (above) and the notes (drop the vector-store, DashScope→Bedrock, and manual-trigger prep).
- Slide 11: embed the muted video, starting on click. Notes: "[8:30–15:30] narrate from demo-script.md". Keep a local copy of the video file on the presenting laptop.
- Slide 12: the new tie-back line (above).
- Slide 18 (hidden backup): no longer needed, since the video is the demo.
