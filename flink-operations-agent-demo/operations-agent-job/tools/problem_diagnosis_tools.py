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
import os
from datetime import datetime, timedelta
from pathlib import Path

import requests

from tools.tool_utils import get_session_with_retry


class ProblemDiagnosisTools:
    """Problem Diagnosis Tools."""

    @staticmethod
    def get_job_exceptions(job_id: str, base_url: str, max_exceptions: int = 10) -> str:
        """Retrieve exception history for analyzing job failures.

        Args:
            job_id: Flink job ID
            base_url: Flink REST API URL
            max_exceptions: Max exceptions to return (default: 10)

        Returns:
            JSON string containing:
            - job_id: Job identifier
            - hasExceptions: Whether the job has any exceptions
            - exceptionCount: Number of exceptions (only when hasExceptions=true)
            - truncated: Whether the exception list was truncated
            - exceptions: Array of exception details (only when hasExceptions=true)
                - timestamp: Exception timestamp in milliseconds
                - exceptionName: Exception class name (e.g., "org.apache.flink.runtime.JobException")
                - taskName: Failed task name (may be null for job-level exceptions)
                - location: Task location (may be null)
                - stacktrace: Full stack trace string
                - concurrentExceptions: Array of concurrent exceptions
                    - exceptionName: Exception class name
                    - taskName: Task name where exception occurred
                    - location: Task location
        """
        try:
            exceptions_url = f"{base_url}/jobs/{job_id}/exceptions"
            session = get_session_with_retry()
            response = session.get(exceptions_url, timeout=30)
            response.raise_for_status()
            exceptions_info = response.json()

            exception_history = exceptions_info.get("exceptionHistory", {})
            entries = exception_history.get("entries", [])
            truncated = exception_history.get("truncated", False)

            if not entries:
                all_exceptions = exceptions_info.get("all-exceptions", [])
                root_exception = exceptions_info.get("root-exception")
                timestamp = exceptions_info.get("timestamp")

                result = {
                    "job_id": job_id,
                    "hasExceptions": bool(root_exception or all_exceptions),
                    "rootException": root_exception[:1000] if root_exception else None,
                    "timestamp": timestamp,
                    "allExceptions": all_exceptions[:max_exceptions],
                    "totalExceptions": len(all_exceptions),
                    "truncated": exceptions_info.get("truncated", False),
                    "usingDeprecatedFields": True
                }
            else:
                formatted_entries = []
                for entry in entries[:max_exceptions]:
                    formatted_entry = {
                        "timestamp": entry.get("timestamp"),
                        "exceptionName": entry.get("exceptionName"),
                        "taskName": entry.get("taskName"),
                        "location": entry.get("location"),
                        "stacktrace": entry.get("stacktrace", "")[:1000] if entry.get("stacktrace") else None
                    }

                    concurrent_exceptions = entry.get("concurrentExceptions", [])
                    if concurrent_exceptions:
                        formatted_entry["concurrentExceptions"] = [
                            {
                                "exceptionName": ce.get("exceptionName"),
                                "taskName": ce.get("taskName"),
                                "location": ce.get("location")
                            }
                            for ce in concurrent_exceptions[:5]
                        ]

                    formatted_entries.append(formatted_entry)

                result = {
                    "job_id": job_id,
                    "hasExceptions": len(entries) > 0,
                    "exceptionCount": len(entries),
                    "truncated": truncated,
                    "exceptions": formatted_entries,
                    "usingDeprecatedFields": False
                }

            return json.dumps(result, ensure_ascii=False)

        except requests.exceptions.RequestException as e:
            return json.dumps({
                "error": str(e),
                "job_id": job_id,
            }, ensure_ascii=False)
        except Exception as e:
            return json.dumps({
                "error": str(e),
                "job_id": job_id,
            }, ensure_ascii=False)

    @staticmethod
    def get_physical_vertex_backpressure_details(job_id: str, base_url: str) -> str:
        """Get physical-level backpressure details for operators with backpressure issues.

        This provides a detailed physical execution view of backpressured operators,
        similar to Flink's physical graph. It breaks down each operator into its parallel
        subtasks, showing granular backpressure metrics at the subtask level.

        Use this when you need to identify which specific subtasks are causing backpressure,
        rather than just knowing which operators have issues.

        Note: Backpressure data is only available for RUNNING jobs. Non-running jobs return empty backpressuredOperators.

        Args:
            job_id: Flink job ID to query
            base_url: Flink REST API base URL

        Returns:
            JSON string containing:
            - jobId: Job identifier
            - jobName: Job name
            - jobState: Job state (RUNNING, FAILED, CANCELED, FINISHED)
            - backpressuredOperators: Array of operators with backpressure issues (empty if no issues or job not running)
                - operator_id: Operator vertex ID
                - operator_name: Operator name (e.g., "Source: datagen")
                - parallelism: Operator parallelism
                - level: Aggregated backpressure level ("low" | "high")
                - subtasks_bp: Array of physical subtask backpressure details
                    - subtask: Subtask index (0 to parallelism-1)
                    - backpressure-level: Subtask-specific backpressure level
                    - ratio: Backpressure ratio (0.0 to 1.0, higher means more backpressure)
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
            "jobState": status_state,
            "backpressuredOperators": [],
        }

        backpressured_operators = []
        if status_state == "RUNNING":
            vertices = job_info.get("vertices", [])
            try:
                for vertex in vertices:
                    vertex_id = vertex.get("id")
                    vertex_name = vertex.get("name", "Unknown")
                    vertex_parallelism = vertex.get("parallelism", 1)
                    if vertex_id:
                        backpressure_url = f"{base_url}/jobs/{job_id}/vertices/{vertex_id}/backpressure"
                        bp_response = session.get(backpressure_url, timeout=30)
                        if bp_response.status_code == 200:
                            bp_info = bp_response.json()
                            bp_status = bp_info.get("backpressure-level", "ok")
                            if bp_status != "ok":
                                subtasks_bp = []

                                for subtask in bp_info.get("subtasks", []):
                                    ratio = subtask.get("ratio", 0)

                                    # Keep only ratio and busyRatio, remove idleRatio (since busyRatio + idleRatio = 1)
                                    subtask_bp = {
                                        "subtask": subtask.get("subtask"),
                                        "backpressure-level": subtask.get("backpressure-level", "ok"),
                                        "ratio": ratio,
                                    }
                                    subtasks_bp.append(subtask_bp)
                                backpressured_operators.append(
                                    {
                                        "operator_id": vertex_id,
                                        "operator_name": vertex_name,
                                        "parallelism": vertex_parallelism,
                                        "level": bp_status,
                                        "subtasks_bp": subtasks_bp,
                                    }
                                )
            except Exception:
                pass

        result["backpressuredOperators"] = backpressured_operators
        return json.dumps(result, ensure_ascii=False)

    @staticmethod
    def get_recent_watermark_statistics(
        job_id: str,
        duration_seconds: int = 60,
    ) -> str:
        """Get statistical analysis of recent watermark data for a Flink job.

        This tool analyzes watermark metrics from the past N seconds to provide insights
        into data processing lag trends and catch-up speed. It helps diagnose whether
        the job is catching up with real-time data or falling further behind.

        Args:
            job_id: Flink job ID to query
            duration_seconds: Time window in seconds to look back from current time.
                            Default is 60 seconds. For example:
                            - 30: Analyze watermark data from the last 30 seconds
                            - 120: Analyze watermark data from the last 2 minutes

        Returns:
            JSON string containing:
            - job_id: Job identifier
            - has_data: Whether valid data exists for analysis
            - record_count: Number of records used for statistics
            - lag_statistics: Lag metrics
                - min_lag_seconds: Minimum lag observed
                - max_lag_seconds: Maximum lag observed
                - avg_lag_seconds: Average lag across all records
                - latest_lag_seconds: Most recent lag value
            - catch_up_analysis: Data catch-up speed analysis
                - catch_up_speed_seconds_per_second: Rate of lag reduction (negative means falling behind)
                - is_catching_up: Whether the job is catching up with real-time data
                - estimated_catch_up_time_seconds: Estimated time to catch up (null if not catching up)
                - estimated_catch_up_timestamp: ISO timestamp when catch-up is expected (null if not catching up)
            - time_range: Time range of analyzed data
                - earliest: Timestamp of earliest record
                - latest: Timestamp of latest record
        """
        metric_dir = os.getenv("METRIC_HISTORY_DIR")
        if not metric_dir:
            err_msg = "METRIC_HISTORY_DIR environment variable is not set. "
            raise OSError(err_msg)

        history_file = Path(metric_dir) / f"{job_id}_watermark_history.json"

        if not history_file.exists():
            return json.dumps(
                {
                    "job_id": job_id,
                    "has_data": False,
                    "message": f"No history data found. Looking for file: {history_file}",
                },
                ensure_ascii=False,
            )

        try:
            with Path.open(history_file, encoding="utf-8") as f:
                history_data = json.load(f)
        except (OSError, json.JSONDecodeError):
            return json.dumps(
                {
                    "job_id": job_id,
                    "has_data": False,
                    "message": "Failed to load history data",
                },
                ensure_ascii=False,
            )

        records = history_data.get("records", [])

        if not records:
            return json.dumps(
                {
                    "job_id": job_id,
                    "has_data": False,
                    "message": "No history data for statistics",
                },
                ensure_ascii=False,
            )

        # Calculate time range based on duration
        current_time = datetime.now()
        start_time = current_time - timedelta(seconds=duration_seconds)
        start_time_str = start_time.isoformat()

        # Filter records within the duration
        records = [r for r in records if r["timestamp"] >= start_time_str]

        if not records:
            return json.dumps(
                {
                    "job_id": job_id,
                    "has_data": False,
                    "message": f"No data found within the last {duration_seconds} seconds",
                },
                ensure_ascii=False,
            )

        lag_values = [r["lag_seconds"] for r in records if r.get("lag_seconds") is not None]

        if not lag_values:
            return json.dumps(
                {
                    "job_id": job_id,
                    "has_data": False,
                    "message": "No valid lag data for statistics",
                },
                ensure_ascii=False,
            )

        # Calculate basic lag statistics
        min_lag = min(lag_values)
        max_lag = max(lag_values)
        avg_lag = sum(lag_values) / len(lag_values)
        latest_lag = lag_values[-1]

        # Calculate catch-up speed (lag reduction rate)
        catch_up_analysis = ProblemDiagnosisTools._calculate_catch_up_speed(records, lag_values)

        return json.dumps(
            {
                "job_id": job_id,
                "has_data": True,
                "record_count": len(lag_values),
                "lag_statistics": {
                    "min_lag_seconds": round(min_lag, 3),
                    "max_lag_seconds": round(max_lag, 3),
                    "avg_lag_seconds": round(avg_lag, 3),
                    "latest_lag_seconds": round(latest_lag, 3),
                },
                "catch_up_analysis": catch_up_analysis,
                "time_range": {
                    "earliest": records[0]["timestamp"],
                    "latest": records[-1]["timestamp"],
                },
            },
            ensure_ascii=False,
        )

    @staticmethod
    def _calculate_catch_up_speed(records: list, lag_values: list) -> dict:
        """Calculate the speed at which the job is catching up with real-time data.

        Args:
            records: List of watermark records with timestamps
            lag_values: List of lag values in seconds

        Returns:
            Dictionary containing catch-up speed analysis
        """
        if len(records) < 2:
            return {
                "catch_up_speed_seconds_per_second": None,
                "is_catching_up": None,
                "estimated_catch_up_time_seconds": None,
                "estimated_catch_up_timestamp": None,
                "message": "Insufficient data points to calculate catch-up speed (need at least 2 records)",
            }

        # Calculate time span in seconds
        first_timestamp = datetime.fromisoformat(records[0]["timestamp"])
        last_timestamp = datetime.fromisoformat(records[-1]["timestamp"])
        time_span_seconds = (last_timestamp - first_timestamp).total_seconds()

        if time_span_seconds <= 0:
            return {
                "catch_up_speed_seconds_per_second": None,
                "is_catching_up": None,
                "estimated_catch_up_time_seconds": None,
                "estimated_catch_up_timestamp": None,
                "message": "Invalid time span for catch-up speed calculation",
            }

        # Calculate lag change
        first_lag = lag_values[0]
        last_lag = lag_values[-1]
        lag_change = first_lag - last_lag  # Positive means catching up

        # Calculate catch-up speed (seconds of lag reduced per second of real time)
        catch_up_speed = lag_change / time_span_seconds

        is_catching_up = catch_up_speed > 0
        estimated_catch_up_time = None
        estimated_catch_up_timestamp = None

        # Calculate estimated time to catch up if catching up
        if is_catching_up and last_lag > 0:
            # Time needed = remaining lag / catch-up speed
            estimated_catch_up_time = last_lag / catch_up_speed
            estimated_catch_up_timestamp = (
                    datetime.now() + timedelta(seconds=estimated_catch_up_time)
            ).isoformat()

        return {
            "catch_up_speed_seconds_per_second": round(catch_up_speed, 6),
            "is_catching_up": is_catching_up,
            "estimated_catch_up_time_seconds": round(estimated_catch_up_time, 2) if estimated_catch_up_time else None,
            "estimated_catch_up_timestamp": estimated_catch_up_timestamp,
        }
