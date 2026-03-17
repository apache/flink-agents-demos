# Internal Scripts and Resources

This directory contains internal scripts, tools, and configuration files that are used by the main user-facing scripts in the `bin/` directory.

## ⚠️ Important

**These files are not intended to be called directly by users.** They are internal implementation details and should only be invoked by the main scripts in the parent `bin/` directory.

## Contents

### Configuration
- `flink_config.yaml` - Flink configuration file (will be copied as config.yaml during setup)

### Setup Scripts
- `setup_flink.sh` - Sets up and starts Flink cluster
- `setup_infra.sh` - Sets up infrastructure services (Kafka, Elasticsearch)

### Job Management Scripts
- `submit_agent_job.sh` - Submits the operations agent job
- `submit_sample_jobs.sh` - Submits sample Flink jobs for demo

### Monitoring Scripts
- `start_metric_collector.sh` - Starts the metric collector
- `metric_collector.py` - Python script for collecting Flink metrics
- `auto_send_job_info.sh` - Automatically sends job info at intervals
- `auto_send_job_info.py` - Python implementation of auto job info sender

### Utility Scripts
- `consume_operations_record.sh` - Consumes and saves operations records
- `upload_sop_to_elasticsearch.py` - Uploads SOP documents to Elasticsearch vector store

## Usage

These scripts are automatically called by the main user scripts:
- `bin/start_all.sh` - Main entry point to start all services
- `bin/stop_all.sh` - Stops all services
- `bin/submit_custom_job.sh` - Submit custom jobs
- `bin/refresh_vector_store_sop.sh` - Refresh SOP vector store

## For Developers

If you need to modify these internal scripts, ensure that:
1. All path references use `$SCRIPT_DIR`, `$BIN_DIR`, and `$PROJECT_DIR` correctly
2. Changes are tested with the main user scripts
3. Documentation is updated if behavior changes
