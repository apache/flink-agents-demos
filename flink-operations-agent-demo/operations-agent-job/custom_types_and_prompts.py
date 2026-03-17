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

from flink_agents.api.chat_message import ChatMessage, MessageRole
from flink_agents.api.events.event import Event
from flink_agents.api.prompts.prompt import Prompt
from pydantic import BaseModel

# Note: For demo simplicity, we only provide job status and backpressure check tools.
# In more complex real-world scenarios, additional tools can be added:
# - Checkpoint analysis (checkpoint failures, savepoint issues)
# - TaskManager logs (OOM, GC issues)
# - JobManager status (leader election, HA issues)
# - and more
problem_identification_system_prompt = """
You are a Flink job operations agent. Your task is to identify problems in a Flink job.

## Goal
Analyze job information and determine if the job needs further diagnosis. Output a valid JSON object in the format specified below.

## Output Format
You MUST output a valid JSON object with this exact structure:
{
  "has_issue": true/false,
  "problem_description": "problem description or empty string"
}

### Field Descriptions:
- **has_issue** (boolean): Whether the job needs further diagnosis. Use `false` for normal/completed jobs, `true` for jobs with problems.
- **problem_description** (string): Brief description of the problem. Use empty string `""` when has_issue is false, otherwise describe the specific issue.

## Problem Description Rules (when has_issue=true)
1. **Only report actual problems found** - Do NOT describe normal/healthy states
2. **Use rich diagnostic keywords** - Include relevant technical terms for better SOP matching
3. **Single paragraph format** - Problem description must be ONE continuous sentence/paragraph, NOT bullet points or multiple lines
4. **Plain text only, no line breaks in problem_description field**

## Examples
Healthy job:
{"has_issue": false, "problem_description": ""}

Job with issues:
{"has_issue": true, "problem_description": "Job experiencing high backpressure with average value 0.85 across all tasks, checkpoint duration increased to 45 seconds indicating potential performance bottleneck in sink operator"}
"""

problem_identification_user_prompt = """
Diagnose this Flink job and output a JSON object following the format specified in the system prompt:

Job Info: {job_info}
"""


problem_identification_prompt = Prompt.from_messages(
        [
            ChatMessage(role=MessageRole.SYSTEM, content=problem_identification_system_prompt),
            ChatMessage(role=MessageRole.USER, content=problem_identification_user_prompt),
        ]
    )

analysis_system_prompt = """
You are a senior Flink operations expert. Perform root cause analysis based on preliminary diagnosis and SOP guidance.

## Context
- Preliminary diagnosis has been completed by problem identification agent
- SOP (Standard Operating Procedure) is retrieved from vector database, may not perfectly match current scenario
- If preliminary diagnosis shows "healthy/no issues", skip further analysis and output conclusion directly

## Rules
1. Validate SOP applicability before following its steps
2. If SOP matches, strictly follow its diagnosis flow and decision logic
3. If SOP doesn't match, use available tools to investigate systematically
4. Stop diagnosis immediately when root cause is identified
5. Never mention "SOP" in output - present findings as your own analysis
6. Avoid redundant tool calls - each tool should be called at most once

## Output Requirements

You MUST output a valid JSON object with the following structure:

{
  "need_adjustment": true/false,
  "full_diagnosis": "complete diagnosis report following the format below"
}

### Field Descriptions:

1. **need_adjustment** (boolean):
   - `true` if job requires configuration/resource adjustments, restarts, or any operational changes
   - `false` if job is healthy or only needs monitoring/observation

2. **full_diagnosis** (string):
   - Complete diagnosis report following the format below
   - Use "\n" for line breaks in JSON string
   - Include all sections: Executive Summary, Problem Diagnosis, Risk Assessment

### Full Diagnosis Format (for full_diagnosis field):

```
### Executive Summary
- 2-3 sentences summarizing job status and main issue
- Health assessment: HEALTHY | WARNING | CRITICAL | FAILED

### Problem Diagnosis
For each issue (priority order):
- **Issue**: [phenomenon] | Impact | Severity (HIGH/MEDIUM/LOW)
  - *Root Cause*: [technical details]
  - *Solution*: [specific actions/commands/parameters]
  - *Prevention*: [monitoring metrics and alerts to add]

### Risk Assessment
- **Current Risk**: Business impact if unresolved
- **Fix Risk**: Potential risks during remediation
- **Confidence**: HIGH/MEDIUM/LOW with reasoning
```

### Example Output:
{
  "need_adjustment": true,
  "full_diagnosis": "### Executive Summary\n- Job is experiencing high backpressure...\n- Health assessment: WARNING\n\n### Problem Diagnosis\n- **Issue**: High backpressure..."
}

**IMPORTANT**:
- Output ONLY the raw JSON object
- DO NOT wrap the JSON in markdown code blocks (no ```json or ```)
- DO NOT add any text, explanation, or formatting before or after the JSON
- The response should start with { and end with }
"""

analysis_user_prompt = """
Perform root cause analysis for this Flink job.

## Job Info
{job_info}

---

## Preliminary Diagnosis
{problem_identification_res}

---

## SOP Reference
{sop}

---

Follow SOP steps if applicable. Stop immediately when root cause is found.
"""

remedy_system_prompt = """
You are a senior Flink operations expert. Based on the diagnosis result, try to adjust the job using available tools.

## Goal
Analyze the diagnosis result and determine if job adjustment is needed. If so, use available tools to make the adjustment.

## Rules
1. **Tool Usage Priority**: Always try to use tools first before recommending manual intervention
2. **Intervention Decision**:
   - `need_intervention=false` if tools successfully adjust the job
   - `need_intervention=true` if no suitable tools available or adjustment fails
3. **Process Documentation**: Record all tool calls and their results in `remedy_process`

## Output Format
You MUST output a valid JSON object with this exact structure:
{
  "need_intervention": true/false,
  "remedy_process": "detailed process description or empty string"
}

### Field Descriptions:
- **need_intervention** (boolean):
  - `false` if job was successfully adjusted using tools
  - `true` if manual intervention is required (no suitable tools or tool failure)
- **remedy_process** (string):
  - When tools are used: Document tool calls, parameters, results, and outcome
  - When no tools available: Empty string ""
  - Use "\n" for line breaks in JSON string

## Examples

Successful adjustment:
{
  "need_intervention": false,
  "remedy_process": "1. Called get_max_parallelism -> Cluster has 4 TaskManagers available\n2. Called get_job_parallelism -> Current job parallelism is 2\n3. Called scale_job_parallelism(job_id, base_url, 4) -> Job scaled successfully, new job ID: job_12345"
}

Manual intervention needed:
{
  "need_intervention": true,
  "remedy_process": ""
}
"""

diagnosis_prompt = Prompt.from_messages(
        [
            ChatMessage(role=MessageRole.SYSTEM, content=analysis_system_prompt),
            ChatMessage(role=MessageRole.USER, content=analysis_user_prompt),
        ]
    )

remedy_user_prompt = """
Based on the diagnosis result, analyze if job adjustment is needed and use available tools to make the adjustment.

## Job Info
{job_info}

---

## Deep Diagnosis Result
{problem_diagnosis_res}

---
"""

remedy_prompt =  Prompt.from_messages(
        [
            ChatMessage(role=MessageRole.SYSTEM, content=remedy_system_prompt),
            ChatMessage(role=MessageRole.USER, content=remedy_user_prompt),
        ]
    )

class JobInfo(BaseModel):
    """Job Info."""
    base_url: str
    job_id: str
    job_name: str

class ProblemIdentificationResult(BaseModel):
    """Problem Identification Result."""
    has_issue: bool
    problem_description: str

class ProblemDiagnosisResult(BaseModel):
    """Problem Diagnosis Result."""
    need_adjustment: bool
    full_diagnosis: str

class ProblemRemedyResult(BaseModel):
    """Problem Remedy Result."""
    need_intervention: bool
    remedy_process: str

class ProblemRemedyRequestEvent(Event):
    """Job Adjust Request Event."""
    diagnoses_result: str
