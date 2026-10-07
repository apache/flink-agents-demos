---
name: balanced-task-scheduling
description: Runbook for a Flink job where some TaskManagers run more tasks than others and become the bottleneck (backpressure, a hot TaskManager at full CPU while others idle). Covers when to switch taskmanager.load-balance.mode to TASKS, how to verify it against a baseline, how to tune slot.request.max-interval when the balance does not hold after failovers, and when to stop and escalate.
license: Apache-2.0
---

# Balanced Tasks Scheduling runbook (Flink 2.2+, FLIP-370)

Sources:
- https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/deployment/tasks-scheduling/balanced_tasks_scheduling/
- https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/tasks-scheduling/balanced_tasks_scheduling/ (failover tuning note)

## Background
With the default `taskmanager.load-balance.mode: NONE`, slots are filled without
looking at how many tasks each slot carries. When operators have different
parallelism (for example 6 and 3), some slots hold 2 tasks and some hold 1, so one
TaskManager can end up with 4 tasks while another has 2. If tasks are CPU-heavy,
the TaskManager with the most tasks saturates and backpressures the whole job,
while the others idle. Adding machines does not fix this; placement does.

`TASKS` mode waits until all slot requests are known, then assigns the slots with
the most tasks first, each to the TaskManager that currently has the fewest tasks.

## Step 1: Confirm the bottleneck is real and persistent
Only act when the imbalance persists for at least 3 consecutive observation windows:
- task count differs between TaskManagers (skew >= 2), AND
- the TaskManager with the most tasks is near 100% CPU / busy while another is
  well below, AND
- upstream tasks are backpressured.
A single snapshot is not enough: while fewer than 3 windows show it, WAIT. The docs
caution against enabling TASKS when you are not seeing these bottlenecks: it may
degrade the job.

## Step 2: Record a baseline, then switch to TASKS
Before changing anything, record the baseline: tasks per TaskManager, CPU per
TaskManager, and throughput. Then set `taskmanager.load-balance.mode: TASKS`.
In a session cluster this is cluster configuration, so apply it by redeploying the
cluster and restoring the job from a savepoint.

## Step 3: Verify against the baseline
After the job is RUNNING again and metrics have settled:
- KEEP if the task skew dropped and throughput did not get worse.
- REVERT to the previous mode if throughput is lower than the baseline.

## Step 4: If the balance does not hold after failovers
After failovers, delayed updates of the resource view can leave the placement
less balanced than optimal. If TASKS is already on and the skew is back, increase
`slot.request.max-interval` by 50 ms (default 20 ms, so 70, then 120, then 170)
and re-verify after every change. A larger value also raises the risk of hitting
`slot.request.timeout`, so make at most 3 increases.

## Step 5: Stop and escalate
If the job is still unbalanced after 3 increases, stop tuning and report it in
FLINK-38715 (https://issues.apache.org/jira/browse/FLINK-38715). Include the
scheduling configuration and what was observed after each attempt.

## Decision table
| Situation | Action |
|---|---|
| No skew or no backpressure, no open incident | NO_ACTION |
| Skew + hot TaskManager + backpressure for fewer than 3 windows | WAIT |
| Same for 3+ windows, mode is NONE, TASKS not tried yet | APPLY_TASKS (record baseline first) |
| TASKS just applied, cooldown over, throughput >= baseline | KEEP |
| TASKS just applied, cooldown over, throughput < baseline | REVERT |
| TASKS on, skew back after failovers, fewer than 3 increases so far | INCREASE_INTERVAL by 50 ms |
| TASKS on, still skewed after 3 increases | ESCALATE |

## Guardrails
- One change at a time; verify before the next change.
- Never repeat an action that was already applied for the current incident.
- Respect the cooldown after a change before judging its effect.
