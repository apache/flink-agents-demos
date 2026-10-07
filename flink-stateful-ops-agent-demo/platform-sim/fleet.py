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
"""SIMULATED fleet: jobs that emit the same metric samples as the real target job.

All jobs are healthy except fleet/payments-sessionizer once its incident is
triggered: it already runs with TASKS balancing, fails over twice, and comes back
unbalanced (4/3/2). The imbalance persists whatever slot.request.max-interval is
applied (the FLINK-38715 case), so the runbook ends in an escalation.
"""
import random
import threading
import time
import uuid

INCIDENT_JOB = "fleet/payments-sessionizer"
HEALTHY = [
    "ads-click-attribution", "search-query-logger", "feed-ranking-features", "jobs-recommendation",
    "notifications-fanout", "profile-view-counter", "messaging-delivery", "fraud-signals",
    "learning-progress", "sales-lead-scoring", "groups-activity", "events-rsvp",
    "creator-analytics", "hashtag-trends", "connection-suggestions", "company-follow",
    "video-watch-time", "newsletter-opens", "skills-endorsement", "premium-billing",
]
REDEPLOY_STEPS = [("stop-with-savepoint", 1.5), ("recreate cluster", 4.0),
                  ("submit from savepoint", 1.5), ("wait for RUNNING", 1.0)]
# CPU of a TaskManager by its task count while the incident job is unbalanced.
INCIDENT_CPU = {4: 0.97, 3: 0.72, 2: 0.45}


class SimJob:
    def __init__(self, name, rng, tasks, mode="TASKS", interval_ms=20):
        self.name = name
        self.rng = rng
        self.balanced = dict(tasks)
        self.tasks = dict(tasks)
        self.config = {"mode": mode, "interval_ms": interval_ms}
        self.per_task = rng.uniform(0.13, 0.2)
        self.rate_per_task = rng.randint(800, 2500)
        self.records_in = 0.0
        self.job_id = uuid.uuid4().hex
        self.failovers = 0
        self.incident = False
        self.down_until = 0.0
        self.last_ts = None

    def sample(self, ts):
        s = {"job": self.name, "simulated": True, "reachable": True, "config": dict(self.config),
             "recent_failovers": self.failovers}
        if ts < self.down_until:
            self.last_ts = None
            return {**s, "running": False}
        if self.incident:
            cpu = {tm: round(INCIDENT_CPU.get(n, 0.6) + self.rng.uniform(-0.03, 0.02), 2)
                   for tm, n in self.tasks.items()}
        else:
            cpu = {tm: round(n * self.per_task + self.rng.uniform(-0.04, 0.04), 2)
                   for tm, n in self.tasks.items()}
        rate = sum(self.tasks.values()) * self.rate_per_task * (0.62 if self.incident else 1.0)
        if self.last_ts is not None:
            self.records_in += rate * (ts - self.last_ts) * self.rng.uniform(0.97, 1.03)
        self.last_ts = ts
        return {**s, "running": True, "job_id": self.job_id, "tasks_per_tm": dict(self.tasks), "cpu": cpu,
                "backpressure_ms": round(self.rng.uniform(580, 760) if self.incident else self.rng.uniform(0, 60)),
                "records_in": round(self.records_in)}

    def restart(self, down_s):
        self.down_until = time.time() + down_s
        self.job_id = uuid.uuid4().hex
        self.records_in = 0.0


class Fleet:
    def __init__(self, seed=7):
        self.lock = threading.Lock()
        self.seed = seed
        self.reset()

    def reset(self):
        with self.lock:
            rng = random.Random(self.seed)
            self.jobs = {}
            for name in HEALTHY:
                n = rng.choice([2, 3, 3, 4])
                tms = rng.choice([3, 3, 4])
                mode = rng.choice(["TASKS", "TASKS", "NONE"])
                job = SimJob(f"fleet/{name}", rng, {f"tm-{i + 1}": n for i in range(tms)}, mode)
                self.jobs[job.name] = job
            self.jobs[INCIDENT_JOB] = SimJob(INCIDENT_JOB, rng, {"tm-1": 3, "tm-2": 3, "tm-3": 3}, "TASKS", 20)

    def samples(self, ts):
        with self.lock:
            return [job.sample(ts) for job in self.jobs.values()]

    def trigger_incident(self, name=INCIDENT_JOB):
        """Two failovers, after which TASKS balancing leaves the job at 4/3/2."""
        with self.lock:
            job = self.jobs[name]
            job.failovers = 2
            job.incident = True
            job.tasks = {"tm-1": 4, "tm-2": 3, "tm-3": 2}
            job.restart(4)

    def redeploy(self, name, mode, interval_ms, on_step):
        """Simulated redeploy with the same steps as the real one; the imbalance survives it."""
        with self.lock:
            job = self.jobs[name]
            job.restart(sum(d for _, d in REDEPLOY_STEPS) + 1)
        for step, seconds in REDEPLOY_STEPS:
            on_step(step)
            time.sleep(seconds)
            if step == "recreate cluster":
                with self.lock:
                    job.config = {"mode": mode, "interval_ms": int(interval_ms)}
        with self.lock:
            if not job.incident:
                job.tasks = dict(job.balanced)
            return {"job_id": job.job_id, "simulated": True}

    def summary(self):
        now = time.time()
        with self.lock:
            out = []
            for job in self.jobs.values():
                counts = list(job.tasks.values())
                status = "redeploying" if now < job.down_until else ("unbalanced" if job.incident else "healthy")
                out.append({"job": job.name, "status": status, "config": dict(job.config),
                            "tasks_per_tm": dict(job.tasks), "task_skew": max(counts) - min(counts)})
            return out
