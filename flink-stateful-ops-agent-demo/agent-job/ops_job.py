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
"""Operations agent job: Kafka metrics -> event-time windows -> persistence filter -> agent -> Kafka.

Submit with bin/demo.sh agent (flink run -pym ops_job -pyfs <this dir>).
"""
import json
import os
from pathlib import Path

from pyflink.common import SimpleStringSchema, WatermarkStrategy
from pyflink.common.time import Time
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import DeliveryGuarantee
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
)
from pyflink.datastream.window import TumblingEventTimeWindows

from flink_agents.api.execution_environment import AgentsExecutionEnvironment
from ops_agent import COOLDOWN_SECONDS, RUNBOOK_PATH, OpsAgent
from ops_pipeline import PersistenceFilter, SummarizeWindow

KAFKA = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
METRICS_TOPIC = "ops_metrics"
RECORDS_TOPIC = "ops_records"
WINDOW_SECONDS = int(os.environ.get("WINDOW_SECONDS", "15"))
COOLDOWN = int(os.environ.get("COOLDOWN_SECONDS", "30"))
AGENT_PARALLELISM = 4
# flink run may execute a copy of this file, so bin/demo.sh passes the absolute path.
RUNBOOK = os.environ.get(
    "OPS_RUNBOOK",
    str(Path(__file__).resolve().parent.parent / "skills" / "balanced-task-scheduling" / "SKILL.md"),
)

if __name__ == "__main__":
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(AGENT_PARALLELISM)
    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA)
        .set_topics(METRICS_TOPIC)
        .set_group_id("ops-agent")
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )
    samples = (
        # Event time = the Kafka record timestamp, set by the collector to the poll time.
        # After a crash the replayed samples land in the same windows, so the agent sees
        # identical inputs and its action store can replay instead of re-executing.
        env.from_source(source, WatermarkStrategy.for_monotonous_timestamps(), "metrics (Kafka)")
        .set_parallelism(1)
        .map(lambda s: json.loads(s))
        .name("parse")
        .set_parallelism(1)
    )
    windows = (
        samples.key_by(lambda s: s["job"])
        .window(TumblingEventTimeWindows.of(Time.seconds(WINDOW_SECONDS)))
        .process(SummarizeWindow())
        .name(f"window {WINDOW_SECONDS}s")
        .set_parallelism(1)
    )
    persistent = (
        windows.key_by(lambda w: w["job"])
        .process(PersistenceFilter())
        .name("persistence filter")
        .set_parallelism(1)
    )

    agents_env = AgentsExecutionEnvironment.get_execution_environment(env)
    agents_env.get_config().set_str(RUNBOOK_PATH, str(RUNBOOK))
    agents_env.get_config().set_int(COOLDOWN_SECONDS, COOLDOWN)
    records = (
        agents_env.from_datastream(persistent, key_selector=lambda w: w["job"])
        .apply(OpsAgent())
        .to_datastream(output_type=Types.STRING())
    )
    records.sink_to(
        KafkaSink.builder()
        .set_bootstrap_servers(KAFKA)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(RECORDS_TOPIC)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build()
    ).name("records (Kafka)")
    agents_env.execute()
