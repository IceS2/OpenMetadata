#  Copyright 2021 Collate
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#  http://www.apache.org/licenses/LICENSE-2.0
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
"""
Base workflow definition.
"""

import uuid
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Dict, List, Optional, TypeVar, Union

from metadata.generated.schema.api.services.ingestionPipelines.createIngestionPipeline import (
    CreateIngestionPipelineRequest,
)
from metadata.generated.schema.entity.services.connections.metadata.openMetadataConnection import (
    OpenMetadataConnection,
)
from metadata.generated.schema.entity.services.ingestionPipelines.ingestionPipeline import (
    AirflowConfig,
    IngestionPipeline,
    PipelineState,
)
from metadata.generated.schema.entity.services.ingestionPipelines.status import (
    StackTraceError,
)
from metadata.generated.schema.metadataIngestion.workflow import LogLevels
from metadata.generated.schema.tests.testSuite import ServiceType
from metadata.generated.schema.type.entityReference import EntityReference
from metadata.ingestion.api.step import Step
from metadata.ingestion.ometa.client_utils import create_ometa_client
from metadata.ingestion.ometa.ometa_api import OpenMetadata
from metadata.timer.repeated_timer import RepeatedTimer
from metadata.utils import fqn
from metadata.utils.class_helper import (
    get_pipeline_type_from_source_config,
    get_reference_type_from_service_type,
    get_service_class_from_service_type,
)
from metadata.utils.helpers import datetime_to_ts
from metadata.utils.logger import ingestion_logger, set_loggers_level
from metadata.workflow.output_handler import report_ingestion_status
from metadata.workflow.workflow_status_mixin import (
    SUCCESS_THRESHOLD_VALUE,
    WorkflowStatusMixin,
    WorkflowContext,
    IngestionPipelineHandler,
)

from metadata.config.common import WorkflowExecutionError

logger = ingestion_logger()

# Type of service linked to the Ingestion Pipeline
T = TypeVar("T")

REPORTS_INTERVAL_SECONDS = 60


class InvalidWorkflowJSONException(Exception):
    """
    Raised when we cannot properly parse the workflow
    """


class BaseWorkflow(ABC):
    """
    Base workflow implementation
    """

    config: Union[Any, Dict]
    _run_id: Optional[str] = None
    metadata: OpenMetadata
    metadata_config: OpenMetadataConnection
    service_type: ServiceType

    def __init__(
        self,
        config: Union[Any, Dict],
        log_level: LogLevels,
        metadata_config: OpenMetadataConnection,
        service_type: ServiceType,
    ):
        """
        Disabling pylint to wait for workflow reimplementation as a topology
        """
        self.config = config
        self.service_type = service_type
        self._timer: Optional[RepeatedTimer] = None
        self._ingestion_pipeline: Optional[IngestionPipeline] = None
        self._start_ts = datetime_to_ts(datetime.now())

        set_loggers_level(log_level.value)

        # We create the ometa client at the workflow level and pass it to the steps
        self.metadata_config = metadata_config
        self.metadata = create_ometa_client(metadata_config)


        self.post_init()

        self.workflow_context = WorkflowContext.create(
            service_name=self.config.source.serviceName,
            service_type=service_type,
            ingestion_pipeline_fqn=self.config.ingestionPipelineFQN,
            source_config=self.config.source.sourceConfig,
            steps=self.workflow_steps()
        )
        self.ingestion_pipeline_handler = IngestionPipelineHandler.from_workflow_context(
            self.workflow_context,
            self.metadata
        )


    def stop(self) -> None:
        """
        Main stopping logic
        """
        # Stop the timer first. This runs in a separate thread and if not properly closed
        # it can hung the workflow
        self.timer.stop()
        self.metadata.close()

        for step in self.workflow_steps():
            try:
                step.close()
            except Exception as exc:
                logger.warning(f"Error trying to close the step {step} due to [{exc}]")

    @property
    def timer(self) -> RepeatedTimer:
        """
        Status timer: It will print the source & sink status every `interval` seconds.
        """
        if not self._timer:
            self._timer = RepeatedTimer(
                REPORTS_INTERVAL_SECONDS, report_ingestion_status, logger, self
            )

        return self._timer

    @classmethod
    @abstractmethod
    def create(cls, config_dict: dict):
        """Single function to execute to create a Workflow instance"""

    @abstractmethod
    def post_init(self) -> None:
        """Method to execute after we have initialized all the internals"""

    @abstractmethod
    def execute_internal(self) -> None:
        """Workflow-specific logic to execute safely"""

    @abstractmethod
    def calculate_success(self) -> float:
        """Get the success % of the internal execution"""

    @abstractmethod
    def get_failures(self) -> List[StackTraceError]:
        """Get the failures to flag whether if the workflow succeeded or not"""

    @abstractmethod
    def workflow_steps(self) -> List[Step]:
        """Steps to report status from"""

    @abstractmethod
    def raise_from_status_internal(self, raise_warnings=False) -> None:
        """Based on the internal workflow status, raise a WorkflowExecutionError"""

    def execute(self) -> None:
        """
        Main entrypoint:
        1. Start logging timer. It will be closed at `stop`
        2. Execute the workflow
        3. Validate the pipeline status
        4. Update the pipeline status at the end
        """
        pipeline_state = PipelineState.success
        self.timer.trigger()
        try:
            self.ingestion_pipeline_handler.set_status(state=PipelineState.running)
            self.execute_internal()

            if SUCCESS_THRESHOLD_VALUE <= self.calculate_success() < 100:
                pipeline_state = PipelineState.partialSuccess

        # Any unhandled exception breaking the workflow should update the status
        except Exception as err:
            pipeline_state = PipelineState.failed
            raise err

        # Force resource closing. Required for killing the threading
        finally:
            ingestion_status = self.ingestion_pipeline_handler.build_ingestion_status()
            self.ingestion_pipeline_handler.set_status(pipeline_state, ingestion_status)
            self.stop()

    @property
    def run_id(self) -> str:
        """
        If the config does not have an informed run id, we'll
        generate and assign one here.
        """
        return self.workflow_context.run_id

    def raise_from_status(self, raise_warnings=False):
        """
        Method to raise error if failed execution
        and updating Ingestion Pipeline Status
        """
        try:
            self.raise_from_status_internal(raise_warnings)
        except WorkflowExecutionError as err:
            self.ingestion_pipeline_handler.set_status(PipelineState.failed)
            raise err

    def result_status(self) -> int:
        """
        Returns 1 if source status is failed, 0 otherwise.
        """
        if self.get_failures():
            return 1
        return 0
