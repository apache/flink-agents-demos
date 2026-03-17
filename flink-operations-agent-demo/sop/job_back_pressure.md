---
id: job_back_pressure
tag: BACKPRESSURE
description: Flink job running with backpressure issues, high latency, slow processing, data ingestion outpacing capacity, operator bottleneck, processing lag
---

# Backpressure / High Latency Diagnosis

## Diagnosis Flow

### Step 1: Check for Active Exceptions
- **Action**: First check if there are any active exceptions in the job
- **If exceptions found**: Job errors are causing processing to stop
  - **Solution**: Fix the error first before addressing backpressure
  - **Severity**: HIGH → Stop backpressure diagnosis and handle exceptions

### Step 2: Analyze Backpressure Patterns and Identify Root Cause
- **Action**: Check backpressure details for each operator and examine watermark lag to understand processing delays

Below are some common scenarios for reference:

**Scenario 1: Job is Catching Up After Recovery**
- **Phenomenon**: 
  - Lag is gradually decreasing over time (trend is improving)
  - Watermark is significantly behind the current time (still processing historical data)
  - Backpressure may be present, but the key indicator is **lag reduction rate**
  - **Critical**: If lag is decreasing at a reasonable rate (e.g., reducing by >10% per hour), this indicates healthy catch-up progress
- **Root Cause**: Job is recovering from checkpoint/savepoint and consuming backlog data
- **Solution**: Wait for data to catch up, no immediate action needed. Monitor the lag reduction trend.
- **Severity**: MEDIUM
- **Note**: Even if operators show high backpressure or are fully busy, as long as lag is consistently decreasing, the job is catching up successfully

**Scenario 2: Processing Capacity Insufficient**
- **Phenomenon**:
  - Lag is continuously increasing or staying high (NOT decreasing)
  - Backpressure is present at certain operators, indicating their **downstream operators** cannot keep up with the processing rate
  - The backpressured operators' downstream is running at near full capacity (very busy)
  - System cannot keep up with incoming data rate
  - **Critical**: Lag reduction rate is zero or negative (lag is growing)
- **Root Cause**: The downstream operators of the backpressured operators have insufficient processing capacity for the data volume
- **Solution**: Increase the **overall parallelism of the entire Flink job** (not just individual operators) to improve throughput across all operators
- **Severity**: HIGH → Immediate action required

**Scenario 3: No Backpressure Issues**
- **Phenomenon**: 
  - No backpressure detected across any operators
  - Processing is smooth and lag is minimal
- **Status**: Healthy, no backpressure issues
- **Severity**: LOW → End diagnosis