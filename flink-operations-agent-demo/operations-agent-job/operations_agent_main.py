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
#################################################################################
import logging

from flink_agents.api.execution_environment import AgentsExecutionEnvironment
from flink_agents.api.resource import ResourceType
from flink_agents.api.tools.tool import Tool
from pyflink.common import SimpleStringSchema, WatermarkStrategy
from pyflink.common.typeinfo import BasicTypeInfo
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import DeliveryGuarantee
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
)

from operations_agent import FlinkJobOperationsAgent, JobInfo
from tools.operations_agent_tools import OperationsAgentTools


def _to_job_info(json_str: str) -> JobInfo|None:
    try:
        return JobInfo.model_validate_json(json_str)
    except Exception:
        logging.exception(f"Failed to convert {json_str} to JobInfo")
        return None

if __name__ == '__main__':
    env = StreamExecutionEnvironment.get_execution_environment().set_parallelism(1)

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers("localhost:9092")
        .set_topics("job_info")
        .set_group_id("agent_id")
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    src = env.from_source(source, WatermarkStrategy.no_watermarks(), "Kafka Source").map(_to_job_info)

    agent_env = AgentsExecutionEnvironment.get_execution_environment(env)

    for resource_name, func in OperationsAgentTools.parse_operations_tools().items():
        agent_env.add_resource(resource_name, ResourceType.TOOL, Tool.from_callable(func))
        print(f"✅ Registered {resource_name} tool")


    # Create agent instance
    print("📋 Creating FlinkJobOperationsAgent instance...")
    agent = FlinkJobOperationsAgent()


    res = (
        agent_env.from_datastream(src, key_selector=lambda x: 1)
        .apply(agent)
        .to_datastream()
        .map(lambda res: str(res), output_type=BasicTypeInfo.STRING_TYPE_INFO())
    )

    sink = (
        KafkaSink.builder()
        .set_bootstrap_servers("localhost:9092")
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic("operations_record")
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build()
    )

    res.sink_to(sink)

    agent_env.execute()
