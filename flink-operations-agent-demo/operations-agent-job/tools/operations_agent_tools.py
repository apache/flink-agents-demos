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
from typing import Callable

from tools.problem_diagnosis_tools import ProblemDiagnosisTools
from tools.problem_identification_tools import ProblemIdentificationTools
from tools.problem_remedy_tools import ProblemRemedyTools


class OperationsAgentTools:
    """Operations Agent Tools - Collection of tools for operations agent."""

    @staticmethod
    def parse_operations_tools() -> dict[str, Callable]:
        """Get all diagnosis tools."""
        return {
            ProblemIdentificationTools.get_job_status.__name__: ProblemIdentificationTools.get_job_status,
            ProblemIdentificationTools.get_logical_vertex_backpressure_status.__name__: ProblemIdentificationTools.get_logical_vertex_backpressure_status,
            ProblemDiagnosisTools.get_job_exceptions.__name__: ProblemDiagnosisTools.get_job_exceptions,
            ProblemDiagnosisTools.get_physical_vertex_backpressure_details.__name__: ProblemDiagnosisTools.get_physical_vertex_backpressure_details,
            ProblemDiagnosisTools.get_recent_watermark_statistics.__name__: ProblemDiagnosisTools.get_recent_watermark_statistics,
            ProblemRemedyTools.get_job_parallelism.__name__: ProblemRemedyTools.get_job_parallelism,
            ProblemRemedyTools.get_max_parallelism.__name__: ProblemRemedyTools.get_max_parallelism,
            ProblemRemedyTools.scale_job_parallelism.__name__: ProblemRemedyTools.scale_job_parallelism
        }

    @staticmethod
    def get_problem_identification_tool_names() -> list[str]:
        """Get tool names for problem identification."""
        return [
            ProblemIdentificationTools.get_job_status.__name__,
            ProblemIdentificationTools.get_logical_vertex_backpressure_status.__name__
        ]

    @staticmethod
    def get_diagnosis_tool_names() -> list[str]:
        """Get tool names for problem identification."""
        return [
            ProblemDiagnosisTools.get_job_exceptions.__name__,
            ProblemDiagnosisTools.get_physical_vertex_backpressure_details.__name__,
            ProblemDiagnosisTools.get_recent_watermark_statistics.__name__
        ]

    @staticmethod
    def get_remedy_tool_names() -> list[str]:
        """Get tool names for problem remedy."""
        return [
            ProblemRemedyTools.get_max_parallelism.__name__,
            ProblemRemedyTools.get_job_parallelism.__name__,
            ProblemRemedyTools.scale_job_parallelism.__name__
        ]
