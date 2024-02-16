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
Add methods to the workflows for updating the IngestionPipeline status
"""
import traceback
import uuid
from datetime import datetime
from typing import Optional, List, Tuple, TypeVar, Union

from metadata.config.common import WorkflowExecutionError
from metadata.generated.schema.entity.services.ingestionPipelines.ingestionPipeline import (
    AirflowConfig,
    IngestionPipeline,
    PipelineState,
    PipelineStatus,
)
from metadata.generated.schema.entity.services.ingestionPipelines.status import (
    IngestionStatus,
    StepSummary,
)
from metadata.generated.schema.metadataIngestion.workflow import (
    OpenMetadataWorkflowConfig,
)
from metadata.ingestion.api.step import Step, Summary
from metadata.ingestion.ometa.ometa_api import OpenMetadata
from metadata.utils.logger import ometa_logger
from metadata.utils.helpers import datetime_to_ts

from metadata.generated.schema.api.services.ingestionPipelines.createIngestionPipeline import (
    CreateIngestionPipelineRequest,
)
from metadata.generated.schema.entity.services.serviceType import ServiceType
from metadata.generated.schema.tests.testSuite import ServiceType as TestSuiteServiceType
from metadata.generated.schema.tests.testSuite import TestSuite
from metadata.generated.schema.type.entityReference import EntityReference
from metadata.utils import fqn
from metadata.utils.class_helper import (
    get_pipeline_type_from_source_config,
    get_reference_type_from_service_type,
    get_service_class_from_service_type,
)

logger = ometa_logger()

SUCCESS_THRESHOLD_VALUE = 90

T = TypeVar("T")

class WorkflowContext:
    def __init__(
        self,
        run_id: str,
        start_ts: int,
        service_name: str,
        service_type: Union[ServiceType, TestSuiteServiceType],
        ingestion_pipeline_fqn: Optional[str] = None,
        source_config = None,
        steps: Optional[List[Step]] = None
    ):
        self.run_id = run_id
        self.start_ts = start_ts
        self.service_name = service_name
        self.service_type = service_type

        self.ingestion_pipeline_fqn = ingestion_pipeline_fqn
        self.source_config = source_config
        self.steps = steps or []

    @classmethod
    def create(
        cls,
        service_name: str,
        service_type: Union[ServiceType, TestSuiteServiceType],
        ingestion_pipeline_fqn: Optional[str],
        source_config,
        steps: Optional[List[Step]]
    ) -> "WorkflowContext":

        return cls(
            run_id=str(uuid.uuid4()),
            start_ts=datetime_to_ts(datetime.now()),
            service_name=service_name,
            service_type=service_type,
            source_config=source_config,
            ingestion_pipeline_fqn=ingestion_pipeline_fqn,
            steps=steps,
        )


class IngestionPipelineHandler:
    def __init__(
        self,
        workflow_context: WorkflowContext,
        metadata: OpenMetadata,
        ingestion_pipeline: Optional[IngestionPipeline] = None
    ):
        self.workflow_context = workflow_context
        self.metadata = metadata
        self.ingestion_pipeline = ingestion_pipeline

    @staticmethod
    def _get_ingestion_pipeline_service(
        metadata: OpenMetadata,
        workflow_context: WorkflowContext
    ) -> Optional[T]:
        """
        Ingestion Pipelines are linked to either an EntityService (DatabaseService, MessagingService,...)
        or a Test Suite.

        Depending on the Source Config Type, we'll need to GET one or the other to create
        the Ingestion Pipeline
        """

        if workflow_context.service_type == TestSuiteServiceType.TestSuite:
            return metadata.get_by_name(
                entity=TestSuite,
                fqn=fqn.build(
                    metadata=None,
                    entity_type=TestSuite,
                    table_fqn=workflow_context.source_config.config.entityFullyQualifiedName
                )
            )

        return metadata.get_by_name(
            entity=get_service_class_from_service_type(workflow_context.service_type),
            fqn=workflow_context.service_name,
        )

    @staticmethod
    def get_or_create_ingestion_pipeline(
        metadata: OpenMetadata,
        workflow_context: WorkflowContext
    ) -> Optional[IngestionPipeline]:
        """
        If we get the `ingestionPipelineFqn` from the `workflowConfig`, it means we want to
        keep track of the status.
        - During the UI deployment, the IngestionPipeline is already created from the UI.
        - From external deployments, we might need to create the Ingestion Pipeline the first time
          the YAML is executed.
        If the Ingestion Pipeline is not created, create it now to update the status.

        Note that during the very first run, the service might not even be created yet. In that case,
        we won't be able to flag the RUNNING status. We'll wait until the metadata ingestion
        workflow has prepared the necessary components, and we will update the SUCCESS/FAILED
        status at the end of the flow.
        """
        try:
            maybe_pipeline: Optional[IngestionPipeline] = metadata.get_by_name(
                entity=IngestionPipeline, fqn=workflow_context.ingestion_pipeline_fqn
            )

            if maybe_pipeline:
                return maybe_pipeline

            # Get the name from <service>.<name> or, for test suites, <tableFQN>.testSuite
            *_, pipeline_name = fqn.split(workflow_context.ingestion_pipeline_fqn)

            service = IngestionPipelineHandler._get_ingestion_pipeline_service(metadata, workflow_context)

            if service is not None:
                return metadata.create_or_update(
                    CreateIngestionPipelineRequest(
                        name=pipeline_name,
                        service=EntityReference(
                            id=service.id,
                            type=get_reference_type_from_service_type(
                                workflow_context.service_type
                            ),
                        ),
                        pipelineType=get_pipeline_type_from_source_config(
                            workflow_context.source_config.config
                        ),
                        sourceConfig=workflow_context.source_config,
                        airflowConfig=AirflowConfig(),
                    )
                )
        except Exception as exc:
            logger.error(
                f"Error trying to get or create the Ingestion Pipeline due to [{exc}]"
            )

        return None

    @classmethod
    def from_workflow_context(
        cls,
        workflow_context: WorkflowContext,
        metadata: OpenMetadata
    ) -> "IngestionPipelineHandler":

        ingestion_pipeline = cls.get_or_create_ingestion_pipeline(
            metadata,
            workflow_context,
        )

        return cls(
            workflow_context=workflow_context,
            metadata=metadata,
            ingestion_pipeline=ingestion_pipeline
        )

    def _new_pipeline_status(self, state: PipelineState) -> PipelineStatus:
        return PipelineStatus(
            runId=self.workflow_context.run_id,
            pipelineState=state,
            startDate=self.workflow_context.start_ts,
            timestamp=self.workflow_context.start_ts,
        )

    def build_ingestion_status(self) -> Optional[IngestionStatus]:
        """
        Get the results from the steps and prep the payload
        we'll send to the API
        """

        return IngestionStatus(
            __root__=[
                StepSummary.parse_obj(Summary.from_step(step).dict())
                for step in self.workflow_context.steps
            ]
        )

    def set_status(
        self,
        state: PipelineState,
        ingestion_status: Optional[IngestionStatus] = None
    ):
        """
        Method to set the pipeline status of current ingestion pipeline
        """

        try:
            # if we don't have a related Ingestion Pipeline FQN, no status is set.
            # NOTE: Why do we need to check for the ingestion_pipeline_fqn??
            if self.workflow_context.ingestion_pipeline_fqn and self.ingestion_pipeline:
                pipeline_status = self.metadata.get_pipeline_status(
                    self.ingestion_pipeline.fullyQualifiedName.__root__,
                    self.workflow_context.run_id
                )

                if not pipeline_status:
                    # We need to crete the status
                    pipeline_status = self._new_pipeline_status(state)
                else:
                    # if workflow is ended then update the end date in status
                    pipeline_status.endDate = datetime.now().timestamp() * 1000
                    pipeline_status.pipelineState = state

                pipeline_status.status = (
                    ingestion_status if ingestion_status else pipeline_status.status
                )
                self.metadata.create_or_update_pipeline_status(
                    self.ingestion_pipeline.fullyQualifiedName.__root__, pipeline_status
                )
        except Exception as err:
            logger.debug(traceback.format_exc())
            logger.error(
                f"Unhandled error trying to update Ingestion Pipeline status [{err}]"
            )
