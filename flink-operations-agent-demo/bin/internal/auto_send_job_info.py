#!/usr/bin/env python3
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
#  limitations under the License.
################################################################################

import argparse
import json
import logging
import os
import signal
import subprocess
import sys
import time
from datetime import datetime

import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

DEFAULT_FLINK_REST_BASE_URL = "http://localhost:8082"
DEFAULT_SEND_INTERVAL = 5  # seconds
DEFAULT_DURATION = 180  # 3 minutes in seconds
DEFAULT_PID_FILE = "/tmp/auto_send_job_info.pid"


def fetch_running_job_ids(base_url: str = DEFAULT_FLINK_REST_BASE_URL) -> list[dict]:
    """Fetch all RUNNING job IDs and names via Flink REST API.

    Args:
        base_url: Flink REST API URL

    Returns:
        List of dicts containing job_id and job_name
    """
    try:
        response = requests.get(f"{base_url}/jobs/overview", timeout=10)
        response.raise_for_status()
        overview_data = response.json()
        running_jobs = [
            {"job_id": job["jid"], "job_name": job.get("name", "")}
            for job in overview_data.get("jobs", [])
            if job.get("state") == "RUNNING" or job.get("state") == "FAILED"
        ]
        logger.info("Fetched %d running job(s) from %s", len(running_jobs), base_url)
        return running_jobs
    except requests.exceptions.RequestException:
        logger.exception("Failed to fetch job list from %s", base_url)
        return []


def send_job_info_to_kafka(job_id: str, job_name: str, base_url: str) -> bool:
    """Send job info message to Kafka topic 'job_info'.

    Args:
        job_id: Flink job ID
        job_name: Flink job name
        base_url: Flink REST API base URL

    Returns:
        True if message sent successfully, False otherwise
    """
    message = {"job_id": job_id, "job_name": job_name, "base_url": base_url}
    message_json = json.dumps(message)
    
    try:
        result = subprocess.run(
            [
                "docker", "exec", "-i", "kafka",
                "/opt/kafka/bin/kafka-console-producer.sh",
                "--bootstrap-server", "localhost:9092",
                "--topic", "job_info"
            ],
            input=message_json,
            capture_output=True,
            text=True,
            timeout=10
        )
        
        if result.returncode == 0:
            logger.info("Message sent to Kafka: %s", message_json)
            return True
        else:
            logger.error("Failed to send message. stderr: %s", result.stderr)
            return False
    except subprocess.TimeoutExpired:
        logger.error("Timeout while sending message to Kafka")
        return False
    except Exception as e:
        logger.exception("Error sending message to Kafka: %s", e)
        return False


def auto_send_job_info(
    base_url: str = DEFAULT_FLINK_REST_BASE_URL,
    interval: int = DEFAULT_SEND_INTERVAL,
    duration: int = DEFAULT_DURATION,
    pid_file: str = DEFAULT_PID_FILE,
    max_messages: int | None = None,
) -> None:
    """Automatically fetch running job IDs and send job_info to Kafka.

    Args:
        base_url: Flink REST API base URL
        interval: Interval between sends in seconds
        duration: Total duration in seconds
        pid_file: PID file path for process management
        max_messages: Maximum number of messages to send (None for unlimited)
    """
    logger.info("=" * 60)
    logger.info("  Auto Send Job Info to Kafka")
    logger.info("=" * 60)
    logger.info("  Flink REST API: %s", base_url)
    logger.info("  Send interval:  %d seconds", interval)
    logger.info("  Duration:       %d seconds (%.1f minutes)", duration, duration / 60)
    if max_messages:
        logger.info("  Max messages:   %d", max_messages)
    logger.info("  PID file:       %s", pid_file)
    logger.info("=" * 60)
    logger.info("")

    # Write PID file
    try:
        with open(pid_file, "w") as f:
            f.write(str(os.getpid()))
        logger.info("PID file created: %s (PID: %d)", pid_file, os.getpid())
    except OSError as e:
        logger.error("Failed to create PID file: %s", e)
        return

    start_time = time.time()
    message_count = 0
    success_count = 0
    stop_requested = False

    # Signal handler for graceful shutdown
    def signal_handler(signum, frame):
        nonlocal stop_requested
        stop_requested = True
        logger.info("")
        logger.info("Received signal %d. Gracefully shutting down...", signum)

    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        while not stop_requested:
            elapsed = time.time() - start_time
            remaining = duration - elapsed

            # Check if duration reached
            if remaining <= 0:
                logger.info("=" * 60)
                logger.info("  Duration reached. Stopping...")
                logger.info("=" * 60)
                break

            # Check if max messages reached
            if max_messages and message_count >= max_messages:
                logger.info("=" * 60)
                logger.info("  Max messages reached. Stopping...")
                logger.info("=" * 60)
                break

            # Fetch running job IDs
            running_jobs = fetch_running_job_ids(base_url)
            
            if not running_jobs:
                logger.warning("No running jobs found at %s", base_url)
                logger.info("Waiting %d seconds before next attempt...", interval)
                if stop_requested:
                    break
                time.sleep(interval)
                continue

            # Send job info for each running job
            for job_info in running_jobs:
                if stop_requested:
                    break
                
                # Check max messages before sending
                if max_messages and message_count >= max_messages:
                    break
                
                job_id = job_info["job_id"]
                job_name = job_info["job_name"]
                
                logger.info("[%s] Sending job info for job: %s (%s)", 
                           datetime.now().strftime("%H:%M:%S"), job_id, job_name)
                
                if send_job_info_to_kafka(job_id, job_name, base_url):
                    success_count += 1
                message_count += 1
                
                # Wait 5 seconds between each job message
                if job_info != running_jobs[-1]:  # Don't wait after the last job
                    if stop_requested:
                        break
                    time.sleep(5)

            if stop_requested:
                break

            # Wait for next interval
            logger.info("Waiting %d seconds before next send...", interval)
            time.sleep(interval)

    except KeyboardInterrupt:
        logger.info("")
        logger.info("Received keyboard interrupt. Stopping...")
    finally:
        # Clean up PID file
        if os.path.exists(pid_file):
            try:
                os.remove(pid_file)
                logger.info("PID file removed: %s", pid_file)
            except OSError:
                pass
        
        logger.info("")
        logger.info("=" * 60)
        logger.info("  Summary")
        logger.info("=" * 60)
        logger.info("  Total messages sent:     %d", message_count)
        logger.info("  Successful messages:     %d", success_count)
        logger.info("  Failed messages:         %d", message_count - success_count)
        logger.info("  Success rate:            %.2f%%", 
                   (success_count / message_count * 100) if message_count > 0 else 0)
        logger.info("  Total runtime:           %.1f seconds", time.time() - start_time)
        logger.info("=" * 60)


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Automatically fetch running job IDs and send job_info to Kafka"
    )
    parser.add_argument(
        "--base-url",
        default=DEFAULT_FLINK_REST_BASE_URL,
        help=f"Flink REST API base URL (default: {DEFAULT_FLINK_REST_BASE_URL})"
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=DEFAULT_SEND_INTERVAL,
        help=f"Interval between sends in seconds (default: {DEFAULT_SEND_INTERVAL})"
    )
    parser.add_argument(
        "--duration",
        type=int,
        default=DEFAULT_DURATION,
        help=f"Total duration in seconds (default: {DEFAULT_DURATION}, 3 minutes)"
    )
    parser.add_argument(
        "--pid-file",
        default=DEFAULT_PID_FILE,
        help=f"PID file path for process management (default: {DEFAULT_PID_FILE})"
    )
    parser.add_argument(
        "--max-messages",
        type=int,
        default=None,
        help="Maximum number of messages to send (default: unlimited)"
    )
    parser.add_argument(
        "--stop",
        action="store_true",
        help="Stop the running auto_send_job_info process"
    )
    parser.add_argument(
        "--status",
        action="store_true",
        help="Check if auto_send_job_info is running"
    )
    
    args = parser.parse_args()
    
    # Handle --stop command
    if args.stop:
        if os.path.exists(args.pid_file):
            try:
                with open(args.pid_file, "r") as f:
                    pid = int(f.read().strip())
                
                # Check if process is running (works on both Linux and macOS)
                try:
                    os.kill(pid, 0)  # Send signal 0 to check if process exists
                except OSError:
                    print(f"Process {pid} is not running")
                    if os.path.exists(args.pid_file):
                        os.remove(args.pid_file)
                    return
                
                print(f"Found running process {pid}, stopping...")
                
                # Send SIGTERM for graceful shutdown
                os.kill(pid, signal.SIGTERM)
                print(f"Sent SIGTERM to process {pid}")
                print(f"Waiting for process to terminate...")
                
                # Wait for process to terminate (max 10 seconds)
                for i in range(10):
                    time.sleep(1)
                    try:
                        os.kill(pid, 0)  # Check if process still exists
                    except OSError:
                        print(f"Process {pid} has terminated gracefully")
                        if os.path.exists(args.pid_file):
                            os.remove(args.pid_file)
                        return
                
                # Force kill if not terminated
                print(f"Process {pid} did not terminate gracefully, sending SIGKILL...")
                os.kill(pid, signal.SIGKILL)
                print(f"Sent SIGKILL to process {pid}")
                
                # Clean up PID file
                if os.path.exists(args.pid_file):
                    os.remove(args.pid_file)
                    
            except (OSError, ValueError) as e:
                print(f"Error stopping process: {e}")
                if os.path.exists(args.pid_file):
                    os.remove(args.pid_file)
        else:
            print("PID file not found. Process may not be running.")
        return
    
    # Handle --status command
    if args.status:
        if os.path.exists(args.pid_file):
            try:
                with open(args.pid_file, "r") as f:
                    pid = int(f.read().strip())
                
                # Check if process is running (works on both Linux and macOS)
                try:
                    os.kill(pid, 0)  # Send signal 0 to check if process exists
                    print(f"auto_send_job_info is running (PID: {pid})")
                except OSError:
                    print(f"PID file exists but process {pid} is not running")
                    os.remove(args.pid_file)
            except (OSError, ValueError) as e:
                print(f"Error checking status: {e}")
        else:
            print("auto_send_job_info is not running")
        return
    
    # Run the auto send job info
    auto_send_job_info(
        base_url=args.base_url,
        interval=args.interval,
        duration=args.duration,
        pid_file=args.pid_file,
        max_messages=args.max_messages
    )


if __name__ == "__main__":
    main()