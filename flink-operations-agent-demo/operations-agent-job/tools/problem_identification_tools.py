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
import json

import requests

from tools.tool_utils import get_session_with_retry


class ProblemIdentificationTools:
    """Job Operations Tools - Collection of tools for job operations."""

    @staticmethod
    def get_job_status(job_id: str, base_url: str) -> str:
        """Get basic status information of a Flink job.

        Args:
            job_id: Flink job ID to query
            base_url: Flink REST API base URL

        Returns:
            JSON string containing:
            - jobId: Job identifier
            - jobName: Job name
            - statusState: Job state (RUNNING, FAILED, CANCELED, FINISHED)
            - startTime: Job start timestamp in milliseconds
            - duration: Job duration in milliseconds
            - job_end_time: Job end timestamp (only for non-RUNNING jobs)
        """
        job_url = f"{base_url}/jobs/{job_id}"
        response = requests.get(job_url, timeout=10)
        response.raise_for_status()
        job_info = response.json()

        status_state = job_info.get("state", "UNKNOWN")
        job_name = job_info.get("name", "Unknown")
        start_time = job_info.get("start-time", 0)
        duration = job_info.get("duration", 0)

        result = {
            "jobId": job_id,
            "jobName": job_name,
            "statusState": status_state,
            "startTime": start_time,
            "duration": duration,
        }

        # Handle finished jobs
        if status_state != "RUNNING":
            result["job_end_time"] = job_info.get("end-time", job_info.get("endTime"))

        return json.dumps(result, ensure_ascii=False)

    @staticmethod
    def get_logical_vertex_backpressure_status(job_id: str, base_url: str) -> str:
        """Get logical-level backpressure status for all operators in a Flink job.

        This provides a high-level overview of backpressure at the operator (vertex) level,
        similar to Flink's logical graph view. Each operator is represented as a single entity
        with an aggregated backpressure status.

        Note: Backpressure data is only available for RUNNING jobs.

        Args:
            job_id: Flink job ID to query
            base_url: Flink REST API base URL

        Returns:
            JSON string containing:
            - jobId: Job identifier
            - jobName: Job name
            - backpressure_status: Dict mapping operator names to aggregated backpressure levels ("ok" | "low" | "high")
        """
        job_url = f"{base_url}/jobs/{job_id}"
        session = get_session_with_retry()
        response = session.get(job_url, timeout=30)
        response.raise_for_status()
        job_info = response.json()

        status_state = job_info.get("state", "UNKNOWN")
        job_name = job_info.get("name", "Unknown")

        result = {
            "jobId": job_id,
            "jobName": job_name,
            "backpressure_status": {},
        }

        backpressure_status = {}
        if status_state != "RUNNING":
            backpressure_status["No Running Operator"] = (
                "The backpressure level is not applicable when the job is not running."
            )
        else:
            vertices = job_info.get("vertices", [])
            try:
                for vertex in vertices:
                    vertex_id = vertex.get("id")
                    vertex_name = vertex.get("name", "Unknown")
                    if vertex_id:
                        backpressure_url = f"{base_url}/jobs/{job_id}/vertices/{vertex_id}/backpressure"
                        bp_response = session.get(backpressure_url, timeout=30)
                        if bp_response.status_code == 200:
                            bp_info = bp_response.json()
                            bp_status = bp_info.get("backpressure-level", "ok")
                            backpressure_status[vertex_name] = bp_status
                        else:
                            backpressure_status[vertex_name] = "No Backpressure Data"
            except Exception:
                pass

        result["backpressure_status"] = backpressure_status
        return json.dumps(result, ensure_ascii=False)
