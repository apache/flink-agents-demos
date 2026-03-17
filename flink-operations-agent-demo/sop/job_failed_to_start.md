---
id: job_failed_to_start
tag: FAILED
description: Flink job failed to start or crashed due to a critical failure during initialization. There was a job startup error, resulting in an exception or error during launch. The job status is FAILED, with a very short duration indicating a startup failure. No operators are running. The job was terminated immediately after submission.
---

# Job Failed Diagnosis

## Diagnosis Flow

### Step 1: Retrieve Exception Information
- **Action**: Query the job's exception history to understand what went wrong during startup
- **Focus on**: Exception name, which task failed, and the full stack trace

### Step 2: Analyze Exception Type and Determine Root Cause

Below are some common scenarios for reference:

| Symptom | Root Cause | Solution |
|---------|------------|----------|
| Exception indicates no available slots or resources | Cluster resources insufficient | Add TaskManagers or increase resource quota |
| Exception shows class or definition not found | Missing dependency in classpath | Check JAR dependencies and ensure all required libraries are included |
| Exception related to invalid argument or configuration | Configuration error | Review and fix job configuration parameters |
| Exception shows connection failure or timeout to external service | External system unreachable | Check network connectivity and external system status |
| Exception indicates serialization/deserialization failure | Data type or serializer mismatch | Check data types and ensure proper serializers are registered |
| Exception from user code logic (null pointer, runtime error, etc.) | Bug in user code | Debug and fix the user code logic |

### Conclusion
- **Root Cause**: Determined based on the exception symptom analysis
- **Solution**: Apply targeted fix based on the identified exception category
- **Severity**: CRITICAL → Immediate action required
