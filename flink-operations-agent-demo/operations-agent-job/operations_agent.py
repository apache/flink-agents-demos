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
import os
from datetime import datetime

from flink_agents.api.agents.agent import Agent
from flink_agents.api.chat_message import ChatMessage, MessageRole
from flink_agents.api.decorators import (
    action,
    chat_model_connection,
    chat_model_setup,
    embedding_model_connection,
    embedding_model_setup,
    prompt,
    vector_store,
)
from flink_agents.api.events.chat_event import ChatRequestEvent, ChatResponseEvent
from flink_agents.api.events.context_retrieval_event import ContextRetrievalRequestEvent, ContextRetrievalResponseEvent
from flink_agents.api.events.event import Event, InputEvent, OutputEvent
from flink_agents.api.prompts.prompt import Prompt
from flink_agents.api.resource import ResourceDescriptor, ResourceName
from flink_agents.api.runner_context import RunnerContext

from custom_types_and_prompts import (
    JobInfo,
    ProblemDiagnosisResult,
    ProblemIdentificationResult,
    ProblemRemedyRequestEvent,
    ProblemRemedyResult,
    diagnosis_prompt,
    problem_identification_prompt,
    remedy_prompt,
)
from tools.operations_agent_tools import OperationsAgentTools


class FlinkJobOperationsAgent(Agent):
    """Flink Agent for comprehensive job operations.

    This agent performs multi-dimensional analysis of Flink jobs including:
    - Resource analysis
    - Job analysis
    - Exception analysis
    """

    @prompt
    @staticmethod
    def problem_identification_prompt() -> Prompt:
        """System prompt for Flink job problem identification."""
        return problem_identification_prompt

    @prompt
    @staticmethod
    def diagnosis_prompt() -> Prompt:
        """System prompt for Flink job diagnosis analysis."""
        return diagnosis_prompt

    @prompt
    @staticmethod
    def remedy_prompt() -> Prompt:
        """System prompt for Flink job remedy."""
        return remedy_prompt

    @chat_model_connection
    @staticmethod
    def tongyi_connection() -> ResourceDescriptor:
        """TongyiChatModel connection for Flink job problem identification."""
        api_key = os.getenv("DASHSCOPE_API_KEY")
        if not api_key:
            err_msg = "DASHSCOPE_API_KEY environment variable is not set. Please set it in your .env file or environment."
            raise ValueError(err_msg)
        return ResourceDescriptor(clazz=ResourceName.ChatModel.TONGYI_CONNECTION, api_key=api_key, request_timeout=60.0)

    @chat_model_setup
    @staticmethod
    def problem_identification_chat_model() -> ResourceDescriptor:
        """ChatModel setup for Flink job problem identification."""
        return ResourceDescriptor(
            clazz=ResourceName.ChatModel.TONGYI_SETUP,
            connection="tongyi_connection",
            model="qwen-flash",
            prompt="problem_identification_prompt",
            extract_reasoning=True,
            tools=OperationsAgentTools.get_problem_identification_tool_names(),
        )

    @chat_model_setup
    @staticmethod
    def diagnosis_chat_model() -> ResourceDescriptor:
        """ChatModel setup for Flink job diagnosis."""
        return ResourceDescriptor(
            clazz=ResourceName.ChatModel.TONGYI_SETUP,
            connection="tongyi_connection",
            model="qwen-flash",
            prompt="diagnosis_prompt",
            extract_reasoning=True,
            tools=OperationsAgentTools.get_diagnosis_tool_names(),
        )

    @chat_model_setup
    @staticmethod
    def remedy_chat_model() -> ResourceDescriptor:
        """ChatModel setup for Flink job remedy."""
        return ResourceDescriptor(
            clazz=ResourceName.ChatModel.TONGYI_SETUP,
            connection="tongyi_connection",
            model="qwen-flash",
            prompt="remedy_prompt",
            extract_reasoning=True,
            tools=OperationsAgentTools.get_remedy_tool_names(),
        )

    @embedding_model_connection
    @staticmethod
    def embedding_model_connection() -> ResourceDescriptor:
        """EmbeddingModelConnection responsible for ollama model service connection."""
        return ResourceDescriptor(
            clazz=ResourceName.EmbeddingModel.OLLAMA_CONNECTION,
            host="http://localhost:11434",
        )

    @embedding_model_setup
    @staticmethod
    def embedding_model() -> ResourceDescriptor:
        """EmbeddingModel which focus on math, and reuse ChatModelConnection."""
        return ResourceDescriptor(
            clazz=ResourceName.EmbeddingModel.OLLAMA_SETUP,
            connection="embedding_model_connection",
            model=os.environ.get("OLLAMA_EMBEDDING_MODEL", "nomic-embed-text:latest"),
        )

    @vector_store
    @staticmethod
    def vector_store() -> ResourceDescriptor:
        """Vector store setup for knowledge base."""
        return ResourceDescriptor(
            clazz=ResourceName.VectorStore.JAVA_WRAPPER_COLLECTION_MANAGEABLE_VECTOR_STORE,
            java_clazz=ResourceName.VectorStore.Java.ELASTICSEARCH_VECTOR_STORE,
            embedding_model="embedding_model",
            host=os.environ.get("ES_HOST"),
            index="my_documents",
            dims=768,
        )

    @action(InputEvent, ChatResponseEvent)
    @staticmethod
    def simple_problem_identification(event: Event, ctx: RunnerContext) -> None:
        """Perform simple problem identification for job diagnosis.

        Args:
            event: InputEvent containing job diagnosis request, or ChatResponseEvent containing AI problem identification result
            ctx: Runner context for sending events
        """
        # Extract job information from input
        if isinstance(event, InputEvent):
            job_info: JobInfo = event.input
            if job_info is None:
                logging.error("job_info is None, skipping")
                return
            logging.info(f"🔍 Starting diagnosis: {job_info}")

            # Store job information in short-term memory
            ctx.sensory_memory.set("base_url", job_info.base_url)
            ctx.sensory_memory.set("job_id", job_info.job_id)
            ctx.sensory_memory.set("job_name", job_info.job_name)
            ctx.sensory_memory.set("diagnosis_start_time", int(datetime.now().timestamp() * 1000))

            # Send chat request for AI analysis - let LLM decide which tools to use
            logging.info("🤖 Requesting AI problem identification with tool selection...")

            msg = ChatMessage(role=MessageRole.USER, extra_args={"job_info": job_info.model_dump_json()})
            ctx.send_event(ChatRequestEvent(model="problem_identification_chat_model", messages=[msg]))
        elif isinstance(event, ChatResponseEvent):
            sop = ctx.sensory_memory.get("sop")
            problem_diagnosis_res = ctx.sensory_memory.get("problem_diagnosis_res")
            if sop is None and problem_diagnosis_res is None:
                ctx.sensory_memory.set("problem_identification_res", event.response.content)
                logging.info(f"SOP is not set, retrieving SOP with query: {event.response.content}")
                try:
                    result = ProblemIdentificationResult.model_validate_json(event.response.content)
                    if result.has_issue:
                        ctx.send_event(
                            ContextRetrievalRequestEvent(
                                query=result.problem_description, vector_store="vector_store", max_results=1
                            )
                        )
                    else:
                        diagnosis_start_time = ctx.sensory_memory.get("diagnosis_start_time")
                        ctx.send_event(
                            OutputEvent(
                                output={
                                    "job_id": ctx.sensory_memory.get("job_id"),
                                    "job_name": ctx.sensory_memory.get("job_name"),
                                    "base_url": ctx.sensory_memory.get("base_url"),
                                    "status": "completed",
                                    "diagnosis_start_time": datetime.fromtimestamp(diagnosis_start_time / 1000).strftime(
                                        "%Y-%m-%d %H:%M:%S"
                                    ),
                                    "diagnosis_end_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                    "diagnosis_duration": int(datetime.now().timestamp() * 1000) - diagnosis_start_time,
                                    "diagnosis_result": "No issue found.",
                                }
                            )
                        )
                except Exception:
                    # For demo purposes: when model output cannot be parsed as JSON,
                    # we still treat it as a problem case and proceed with diagnosis.
                    # In production, use high-performance models and improve error handling logic.
                    ctx.send_event(
                        ContextRetrievalRequestEvent(
                            query=event.response.content, vector_store="vector_store", max_results=1
                        )
                    )

    @action(ContextRetrievalResponseEvent, ChatResponseEvent)
    @staticmethod
    def deep_problem_analysis(event: Event, ctx: RunnerContext) -> None:
        """Perform deep problem diagnosis using retrieved SOP context.

        Args:
            event: ContextRetrievalResponseEvent containing retrieved SOP, or ChatResponseEvent containing AI diagnosis result
            ctx: Runner context for sending output events
        """
        if isinstance(event, ContextRetrievalResponseEvent):
            sop = event.documents[0].content
            logging.info(f"📚 SOP retrieved: {sop}")
            ctx.sensory_memory.set("sop", sop)

            base_url = ctx.sensory_memory.get("base_url")
            job_id = ctx.sensory_memory.get("job_id")
            job_name = ctx.sensory_memory.get("job_name")
            job_info = JobInfo(base_url=base_url, job_id=job_id, job_name=job_name)
            try:
                sop = ctx.sensory_memory.get("sop")
                problem_identification_res = ctx.sensory_memory.get("problem_identification_res")

                # Send chat request for AI analysis - let LLM decide which tools to use
                logging.info("🤖 Requesting AI analysis with tool selection...")

                msg = ChatMessage(
                    role=MessageRole.USER,
                    extra_args={"job_info": job_info, "sop": sop, "problem_identification_res": problem_identification_res},
                )
                ctx.send_event(ChatRequestEvent(model="diagnosis_chat_model", messages=[msg]))

            except Exception as e:
                error_msg = f"Diagnosis failed for job {job_info.job_id}: {e!s}"
                logging.info(f"❌ {error_msg}")

                ctx.send_event(
                    OutputEvent(
                        output={
                            "job_id": ctx.sensory_memory.get("job_id"),
                            "job_name": ctx.sensory_memory.get("job_name"),
                            "base_url": ctx.sensory_memory.get("base_url"),
                            "status": "failed",
                            "diagnosis_end_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            "diagnosis_result": f"AI analysis failed due to technical error due to {e!s}",
                        }
                    )
                )

        elif isinstance(event, ChatResponseEvent):
            sop = ctx.sensory_memory.get("sop")
            problem_diagnosis_res = ctx.sensory_memory.get("problem_diagnosis_res")
            if sop is not None and problem_diagnosis_res is None:
                try:
                    job_id = ctx.sensory_memory.get("job_id")
                    job_name = ctx.sensory_memory.get("job_name")
                    base_url = ctx.sensory_memory.get("base_url")
                    diagnosis_start_time = ctx.sensory_memory.get("diagnosis_start_time")

                    logging.info(f"🤖 Received AI analysis for job: {job_id}")

                    result = ProblemDiagnosisResult.model_validate_json(event.response.content)

                    if result.need_adjustment:
                        ctx.send_event(
                            ProblemRemedyRequestEvent(diagnoses_result=result.full_diagnosis)
                        )
                    else:
                        # Create output event
                        output_data = {
                            "job_id": job_id,
                            "job_name": job_name,
                            "base_url": base_url,
                            "status": "completed",
                            "diagnosis_start_time": datetime.fromtimestamp(diagnosis_start_time / 1000).strftime("%Y-%m-%d %H:%M:%S"),
                            "diagnosis_end_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            "diagnosis_duration": int(datetime.now().timestamp() * 1000) - diagnosis_start_time,
                            "diagnosis_result": result.full_diagnosis,
                        }

                        logging.info(f"✅ Diagnosis completed for job: {job_id}")
                        logging.info(f"Diagnosis result: {output_data}")
                        ctx.send_event(OutputEvent(output=output_data))

                except Exception as e:
                    error_msg = f"Failed to process AI response: {e!s}"
                    logging.info(f"❌ {error_msg}")

                    ctx.send_event(
                        OutputEvent(
                            output={
                                "job_id": ctx.sensory_memory.get("job_id"),
                                "job_name": ctx.sensory_memory.get("job_name"),
                                "base_url": ctx.sensory_memory.get("base_url"),
                                "status": "failed",
                                "diagnosis_end_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                "diagnosis_result": f"AI analysis failed due to technical error due to {e!s}",
                            }
                        )
                    )

    @action(ProblemRemedyRequestEvent, ChatResponseEvent)
    @staticmethod
    def try_problem_remedy(event: Event, ctx: RunnerContext) -> None:
        """Try to address the problem based on the diagnosis results.

        Args:
            event: ProblemRemedyRequestEvent containing job adjustment request
            ctx: Runner context for sending output events
        """
        if isinstance(event, ProblemRemedyRequestEvent):
            ctx.sensory_memory.set("problem_diagnosis_res", event.diagnoses_result)
            # Send chat request for AI analysis - let LLM decide which tools to use
            logging.info("🤖 Requesting AI job adjustment...")

            base_url = ctx.sensory_memory.get("base_url")
            job_id = ctx.sensory_memory.get("job_id")
            job_name = ctx.sensory_memory.get("job_name")
            job_info = JobInfo(base_url=base_url, job_id=job_id, job_name=job_name)

            msg = ChatMessage(
                role=MessageRole.USER,
                extra_args={"job_info": job_info, "problem_diagnosis_res": event.diagnoses_result},
            )
            ctx.send_event(ChatRequestEvent(model="remedy_chat_model", messages=[msg]))
        elif isinstance(event, ChatResponseEvent):
            problem_diagnosis_res = ctx.sensory_memory.get("problem_diagnosis_res")
            if problem_diagnosis_res is not None:
                try:
                    job_id = ctx.sensory_memory.get("job_id")
                    job_name = ctx.sensory_memory.get("job_name")
                    base_url = ctx.sensory_memory.get("base_url")
                    diagnosis_start_time = ctx.sensory_memory.get("diagnosis_start_time")

                    logging.info(f"🤖 Received AI job adjustment result for job: {job_id}")

                    result = ProblemRemedyResult.model_validate_json(event.response.content)

                    # Create output event
                    output_data = {
                        "job_id": job_id,
                        "job_name": job_name,
                        "base_url": base_url,
                        "status": "completed",
                        "diagnosis_start_time": datetime.fromtimestamp(diagnosis_start_time / 1000).strftime(
                            "%Y-%m-%d %H:%M:%S"
                        ),
                        "diagnosis_end_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "diagnosis_duration": int(datetime.now().timestamp() * 1000) - diagnosis_start_time,
                        "need_intervention": result.need_intervention,
                        "remedy_process": result.remedy_process,
                        "diagnosis_result": problem_diagnosis_res,
                    }

                    logging.info(f"✅ Job adjustment completed for job: {job_id}")
                    logging.info(f"Adjustment result: {output_data}")
                    ctx.send_event(OutputEvent(output=output_data))

                except Exception as e:
                    error_msg = f"Failed to process AI job adjustment response: {e!s}"
                    logging.info(f"❌ {error_msg}")

                    ctx.send_event(
                        OutputEvent(
                            output={
                                "job_id": ctx.sensory_memory.get("job_id"),
                                "job_name": ctx.sensory_memory.get("job_name"),
                                "base_url": ctx.sensory_memory.get("base_url"),
                                "status": "failed",
                                "diagnosis_end_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                "diagnosis_result": f"AI job adjustment failed due to technical error due to {e!s}",
                                "need_intervention": True,
                            }
                        )
                    )
