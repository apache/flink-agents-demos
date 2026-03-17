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
import logging
import os
import threading
import time
from datetime import datetime
from pathlib import Path

import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

DEFAULT_FLINK_REST_BASE_URL = "http://localhost:8082"

# Require METRIC_HISTORY_DIR environment variable
_watermark_dir = os.getenv("METRIC_HISTORY_DIR")
if not _watermark_dir:
    err_msg = "METRIC_HISTORY_DIR environment variable is not set. "
    raise OSError(err_msg)
_watermark_dir = _watermark_dir.strip()
METRIC_HISTORY_DIR = Path(_watermark_dir)


class WatermarkHistoryManager:
    """Manages watermark history data storage."""

    def __init__(self) -> None:
        """Initialize watermark history manager."""
        self.storage_dir = METRIC_HISTORY_DIR
        self.storage_dir.mkdir(parents=True, exist_ok=True)

    def _get_history_file_path(self, job_id: str) -> Path:
        """Get history file path for specific job."""
        return self.storage_dir / f"{job_id}_watermark_history.json"

    def save_watermark_record(
            self,
            job_id: str,
            watermark_ms: int | None,
            lag_seconds: float | None,
    ) -> dict:
        """Save a watermark record to history file.

        Args:
            job_id: Flink job ID
            watermark_ms: Current watermark timestamp in milliseconds
            lag_seconds: Watermark lag in seconds

        Returns:
            Saved record
        """
        record = {
            "timestamp": datetime.now().isoformat(),
            "collect_time_ms": int(time.time() * 1000),
            "watermark_ms": watermark_ms,
            "lag_seconds": lag_seconds,
        }

        history_file = self._get_history_file_path(job_id)
        history_data = self._load_history_file(history_file)

        history_data["job_id"] = job_id
        history_data["records"].append(record)
        history_data["last_updated"] = record["timestamp"]

        self._save_history_file(history_file, history_data)
        return record

    def _load_history_file(self, file_path: Path) -> dict:
        """Load history data file."""
        if file_path.exists():
            try:
                with Path.open(file_path, encoding="utf-8") as f:
                    return json.load(f)
            except (OSError, json.JSONDecodeError):
                pass
        return {"job_id": None, "records": [], "last_updated": None}

    def _save_history_file(self, file_path: Path, data: dict) -> None:
        """Save history data to file."""
        with Path.open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)


watermark_history_manager = WatermarkHistoryManager()


def fetch_running_job_ids(base_url: str = DEFAULT_FLINK_REST_BASE_URL) -> list[str]:
    """Fetch all RUNNING job IDs via Flink REST API.

    Args:
        base_url: Flink REST API URL

    Returns:
        List of RUNNING job IDs
    """
    try:
        response = requests.get(f"{base_url}/jobs/overview", timeout=10)
        response.raise_for_status()
        overview_data = response.json()
        running_job_ids = [
            job["jid"]
            for job in overview_data.get("jobs", [])
            if job.get("state") == "RUNNING"
        ]
        logger.info("Fetched %d running job(s) from %s", len(running_job_ids), base_url)
    except requests.exceptions.RequestException:
        logger.exception("Failed to fetch job list from %s", base_url)
        return []
    else:
        return running_job_ids


def collect_and_save_watermark(
        job_id: str,
        base_url: str = DEFAULT_FLINK_REST_BASE_URL,
        vertices: list | None = None,
) -> str:
    """Collect current watermark data and save to history.

    Args:
        job_id: Flink job ID
        base_url: Flink REST API URL
        vertices: Vertex list, auto-fetch if not provided

    Returns:
        JSON formatted collection result
    """
    try:
        if vertices is None:
            job_url = f"{base_url}/jobs/{job_id}"
            response = requests.get(job_url, timeout=10)
            response.raise_for_status()
            job_data = response.json()
            vertices = job_data.get("vertices", [])

        if not vertices:
            return json.dumps({
                "success": False,
                "job_id": job_id,
                "error": "No vertices found for job",
            }, ensure_ascii=False)

        watermark_ms = None
        lag_seconds = None
        current_time_ms = int(time.time()) * 1000

        for vertex in vertices:
            vertex_id = vertex.get("id")
            if vertex_id:
                try:
                    wm_url = f"{base_url}/jobs/{job_id}/vertices/{vertex_id}/watermarks"
                    wm_response = requests.get(wm_url, timeout=10)
                    if wm_response.status_code == 200:
                        wm_data = wm_response.json()
                        if isinstance(wm_data, list) and len(wm_data) > 0:
                            min_watermark = None
                            for wm in wm_data:
                                wm_value = wm.get("value")
                                if wm_value is not None:
                                    try:
                                        wm_value = int(wm_value)
                                    except (ValueError, TypeError):
                                        continue
                                    if wm_value == -9223372036854775808:
                                        continue
                                    if min_watermark is None or wm_value < min_watermark:
                                        min_watermark = wm_value

                            if min_watermark is not None:
                                watermark_ms = min_watermark
                                lag_seconds = round(max(0, (current_time_ms - min_watermark) / 1000), 3)
                                break
                except Exception:
                    pass

        record = watermark_history_manager.save_watermark_record(
            job_id=job_id,
            watermark_ms=watermark_ms,
            lag_seconds=lag_seconds,
        )

        return json.dumps({
            "success": True,
            "job_id": job_id,
            "saved_record": record,
            "watermark_info": {
                "current_watermark_ms": watermark_ms,
                "watermark_lag_seconds": lag_seconds,
            },
        }, ensure_ascii=False)

    except requests.exceptions.RequestException as e:
        return json.dumps({
            "success": False,
            "job_id": job_id,
            "error": f"Request failed: {e!s}",
        }, ensure_ascii=False)
    except Exception as e:
        return json.dumps({
            "success": False,
            "job_id": job_id,
            "error": str(e),
        }, ensure_ascii=False)


class WatermarkCollectorScheduler:
    """Periodically collect watermark data for all RUNNING jobs.

    Auto-fetch running job IDs via Flink REST API,
    then periodically call collect_and_save_watermark to collect and save watermark data.
    """

    def __init__(
            self,
            base_url: str = DEFAULT_FLINK_REST_BASE_URL,
            interval_seconds: float = 5.0,
    ) -> None:
        """Initialize scheduler.

        Args:
            base_url: Flink REST API URL
            interval_seconds: Collection interval in seconds, default 5
        """
        self.base_url = base_url
        self.interval_seconds = interval_seconds
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    def _collect_loop(self) -> None:
        """Collection loop: periodically fetch job IDs and collect watermark data."""
        logger.info(
            "Watermark collector started (interval=%.1fs, base_url=%s)",
            self.interval_seconds,
            self.base_url,
        )
        while not self._stop_event.is_set():
            try:
                job_ids = fetch_running_job_ids(self.base_url)
                if not job_ids:
                    logger.warning("No running jobs found, will retry in %.1fs", self.interval_seconds)
                else:
                    for job_id in job_ids:
                        if self._stop_event.is_set():
                            break
                        result = collect_and_save_watermark(job_id=job_id, base_url=self.base_url)
                        parsed_result = json.loads(result)
                        if parsed_result.get("success"):
                            lag = parsed_result.get("saved_record", {}).get("lag_seconds")
                            logger.info("Job %s watermark collected, lag=%.3fs", job_id, lag or 0)
                        else:
                            logger.warning("Job %s watermark collection failed: %s", job_id, parsed_result.get("error"))
            except Exception:
                logger.exception("Unexpected error in watermark collection loop")

            self._stop_event.wait(self.interval_seconds)

        logger.info("Watermark collector stopped")

    def start(self) -> None:
        """Start periodic collection (background thread)."""
        if self._thread is not None and self._thread.is_alive():
            logger.warning("Watermark collector is already running")
            return
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._collect_loop, daemon=True, name="watermark-collector")
        self._thread.start()
        logger.info("Watermark collector thread started")

    def stop(self, timeout: float = 10.0) -> None:
        """Stop periodic collection.

        Args:
            timeout: Timeout in seconds to wait for thread termination
        """
        if self._thread is None or not self._thread.is_alive():
            logger.warning("Watermark collector is not running")
            return
        logger.info("Stopping watermark collector...")
        self._stop_event.set()
        self._thread.join(timeout=timeout)
        if self._thread.is_alive():
            logger.warning("Watermark collector thread did not stop within %.1fs", timeout)
        else:
            logger.info("Watermark collector thread stopped")
        self._thread = None

    @property
    def is_running(self) -> bool:
        """Return whether scheduler is running."""
        return self._thread is not None and self._thread.is_alive()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Flink Watermark Collector")
    parser.add_argument("--base-url", default=DEFAULT_FLINK_REST_BASE_URL, help="Flink REST API base URL")
    parser.add_argument("--interval", type=float, default=5.0, help="Collection interval in seconds (default: 5)")
    parser.add_argument("--job-id", default=None, help="Specific job ID to collect (if not set, auto-discover all running jobs)")
    args = parser.parse_args()

    if args.job_id:
        print(collect_and_save_watermark(args.job_id, base_url=args.base_url))
    else:
        scheduler = WatermarkCollectorScheduler(base_url=args.base_url, interval_seconds=args.interval)
        scheduler.start()
        try:
            while scheduler.is_running:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("Received Ctrl+C, shutting down...")
            scheduler.stop()
