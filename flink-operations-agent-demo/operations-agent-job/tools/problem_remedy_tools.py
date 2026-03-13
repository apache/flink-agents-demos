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
import time
from pathlib import Path

import requests


class ProblemRemedyTools:
    """Problem Remedy Tools."""

    @staticmethod
    def get_job_parallelism(job_id: str, base_url: str) -> str:
        """Get parallelism of a Flink job.

        Args:
            job_id: Flink job ID to query
            base_url: Flink REST API base URL

        Returns:
            JSON string containing:
            - jobId: Job identifier
            - jobName: Job name
            - parallelism: Job parallelism
        """
        job_url = f"{base_url}/jobs/{job_id}"
        response = requests.get(job_url, timeout=10)
        response.raise_for_status()
        job_info = response.json()

        job_name = job_info.get("name", "Unknown")
        vertices = job_info.get("vertices", [])

        # Get max parallelism from all vertices
        parallelism = max((v.get("parallelism", 1) for v in vertices), default=1)

        result = {
            "jobId": job_id,
            "jobName": job_name,
            "parallelism": parallelism,
        }

        return json.dumps(result, ensure_ascii=False)

    @staticmethod
    def get_max_parallelism(base_url: str) -> str:
        """Get max parallelism (TaskManager count) of a Flink cluster.

        Args:
            base_url: Flink REST API base URL

        Returns:
            JSON string containing:
            - maxParallelism: Maximum parallelism (TaskManager count)
        """
        overview_url = f"{base_url}/overview"
        response = requests.get(overview_url, timeout=10)
        response.raise_for_status()
        overview_info = response.json()

        tm_count = overview_info.get("taskmanagers", 0)

        result = {
            "maxParallelism": tm_count,
        }

        return json.dumps(result, ensure_ascii=False)

    @staticmethod
    def scale_job_parallelism(job_id: str, base_url: str, new_parallelism: int) -> str:
        """Scale job parallelism by canceling and resubmitting with new parallelism.
        Important Notes:
            - New parallelism should not exceed cluster max parallelism.

        Args:
            job_id: Flink job ID to rescale
            base_url: Flink REST API base URL
            new_parallelism: New parallelism level

        Returns:
            JSON string containing:
            - success: Whether the operation succeeded
            - old_job_id: Original job ID
            - new_job_id: New job ID after resubmission
            - new_parallelism: New parallelism level (only for successful operation)
            - error: Error message if operation failed (only for unsuccessful operation)
        """
        metadata_dir = os.getenv("JOB_METADATA_DIR")
        if not metadata_dir:
            err_msg = "JOB_METADATA_DIR environment variable is not set. "
            raise OSError(err_msg)
        try:
            # Step 1: Load job metadata from file
            metadata_file = Path(metadata_dir) / f"{job_id}.json"

            if not metadata_file.exists():
                return json.dumps(
                    {"success": False, "old_job_id": job_id, "error": f"Job metadata not found: {metadata_file}"},
                    ensure_ascii=False,
                )

            with Path.open(metadata_file) as f:
                metadata = json.load(f)

            jar_id = metadata.get("jar_id")
            entry_class = metadata.get("entry_class")
            job_name = metadata.get("job_name")

            if not jar_id or not entry_class:
                return json.dumps(
                    {"success": False, "old_job_id": job_id, "error": "Invalid metadata: missing jar_id or entry_class"},
                    ensure_ascii=False,
                )

            # Step 2: Cancel current job
            cancel_url = f"{base_url}/jobs/{job_id}"
            cancel_response = requests.patch(cancel_url, timeout=30)
            cancel_response.raise_for_status()

            # Wait for job to be canceled
            max_retries = 30
            retry_count = 0
            while retry_count < max_retries:
                job_url = f"{base_url}/jobs/{job_id}"
                try:
                    job_response = requests.get(job_url, timeout=10)
                    if job_response.status_code == 200:
                        job_state = job_response.json().get("state")
                        if job_state in ["CANCELED", "FAILED", "FINISHED"]:
                            break
                except Exception:
                    break
                time.sleep(1)
                retry_count += 1

            # Step 3: Resubmit job with new parallelism
            run_url = f"{base_url}/jars/{jar_id}/run"
            run_payload = {
                "parallelism": new_parallelism,
                "programArgs": "",
                "entryClass": entry_class,
                "allowNonRestoredState": True
            }

            run_response = requests.post(run_url, json=run_payload, timeout=30)
            run_response.raise_for_status()
            run_info = run_response.json()
            new_job_id = run_info.get("jobid")

            # Step 4: Save new job metadata
            if new_job_id:
                from datetime import datetime

                new_metadata_file = Path(metadata_dir) / f"{new_job_id}.json"
                new_metadata = {
                    "job_id": new_job_id,
                    "jar_id": jar_id,
                    "entry_class": entry_class,
                    "job_name": job_name,
                    "parallelism": new_parallelism,
                    "submit_time": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "rescaled_from": job_id
                }
                with Path.open(new_metadata_file, 'w') as f:
                    json.dump(new_metadata, f, indent=2, ensure_ascii=False)

            result = {
                "success": True,
                "old_job_id": job_id,
                "new_job_id": new_job_id,
                "new_parallelism": new_parallelism,
            }

            return json.dumps(result, ensure_ascii=False)

        except requests.exceptions.RequestException as e:
            return json.dumps(
                {"success": False, "old_job_id": job_id, "error": f"Request error: {e!s}"}, ensure_ascii=False
            )
        except Exception as e:
            return json.dumps(
                {"success": False, "old_job_id": job_id, "error": f"Unexpected error: {e!s}"}, ensure_ascii=False
            )
