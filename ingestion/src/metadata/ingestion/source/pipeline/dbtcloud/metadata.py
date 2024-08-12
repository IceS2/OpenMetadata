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
DBTcloud source to extract metadata from OM UI
"""
import traceback
from datetime import datetime
from typing import Iterable, List, Optional

from metadata.generated.schema.api.data.createPipeline import CreatePipelineRequest
from metadata.generated.schema.api.lineage.addLineage import AddLineageRequest
from metadata.generated.schema.entity.data.pipeline import (
    Pipeline,
    PipelineStatus,
    StatusType,
    Task,
    TaskStatus,
)
from metadata.generated.schema.entity.data.table import Table
from metadata.generated.schema.entity.services.connections.pipeline.dbtCloudConnection import (
    DBTCloudConnection,
)
from metadata.generated.schema.entity.services.ingestionPipelines.status import (
    StackTraceError,
)
from metadata.generated.schema.metadataIngestion.workflow import (
    Source as WorkflowSource,
)
from metadata.generated.schema.type.basic import (
    EntityName,
    FullyQualifiedEntityName,
    Markdown,
    SourceUrl,
    Timestamp,
)
from metadata.generated.schema.type.entityLineage import EntitiesEdge, LineageDetails
from metadata.generated.schema.type.entityLineage import Source as LineageSource
from metadata.generated.schema.type.entityReference import EntityReference
from metadata.ingestion.api.models import Either
from metadata.ingestion.api.steps import InvalidSourceException
from metadata.ingestion.models.pipeline_status import OMetaPipelineStatus
from metadata.ingestion.ometa.ometa_api import OpenMetadata
from metadata.ingestion.source.pipeline.dbtcloud.models import DBTJob
from metadata.ingestion.source.pipeline.pipeline_service import PipelineServiceSource
from metadata.utils import fqn
from metadata.utils.helpers import clean_uri, datetime_to_ts
from metadata.utils.logger import ingestion_logger

logger = ingestion_logger()

STATUS_MAP = {
    "Success": StatusType.Successful.value,
    "Error": StatusType.Failed.value,
    "Cancelled": StatusType.Skipped.value,
    "Running": StatusType.Pending.value,
    "Starting": StatusType.Pending.value,
    "Queued": StatusType.Pending.value,
    0: StatusType.Pending.value,
    1: StatusType.Successful.value,
    2: StatusType.Skipped.value,
}


class DbtcloudSource(PipelineServiceSource):
    """
    Implements the necessary methods ot extract
    Pipeline metadata from DBT cloud
    """

    @classmethod
    def create(
        cls, config_dict, metadata: OpenMetadata, pipeline_name: Optional[str] = None
    ):
        config: WorkflowSource = WorkflowSource.model_validate(config_dict)
        connection: DBTCloudConnection = config.serviceConnection.root.config
        if not isinstance(connection, DBTCloudConnection):
            raise InvalidSourceException(
                f"Expected DBTCloudConnection, but got {connection}"
            )
        return cls(config, metadata)

    def _get_task_list(self, job_id: int) -> Optional[List[Task]]:
        """
        Method to collect all the tasks from dbt cloud job and return it in a task list
        """
        self.context.get().latest_run_id = None
        try:
            task_list: List[Task] = []
            runs = self.client.get_runs(job_id=job_id)
            if runs:
                for run in runs or []:
                    task = Task(
                        name=str(run.id),
                        sourceUrl=SourceUrl(run.href),
                        startDate=str(run.started_at),
                        endDate=str(run.finished_at),
                    )
                    task_list.append(task)
                self.context.get().latest_run_id = (
                    task_list[-1].name if task_list else None
                )
            return task_list or None
        except Exception as exc:
            logger.debug(traceback.format_exc())
            logger.warning(f"Failed to get tasks list due to : {exc}")
        return None

    def yield_pipeline(
        self, pipeline_details: DBTJob
    ) -> Iterable[Either[CreatePipelineRequest]]:
        """
        Method to Get Pipeline Entity
        """
        try:
            connection_url = (
                f"{clean_uri(self.service_connection.host)}/deploy/"
                f"{self.service_connection.accountId}/projects/"
                f"{pipeline_details.project_id}/jobs/{pipeline_details.id}"
            )

            pipeline_request = CreatePipelineRequest(
                name=EntityName(pipeline_details.name),
                description=Markdown(pipeline_details.description),
                sourceUrl=SourceUrl(connection_url),
                tasks=self._get_task_list(job_id=int(pipeline_details.id)),
                scheduleInterval=str(pipeline_details.schedule.cron)
                if pipeline_details.schedule
                else None,
                service=FullyQualifiedEntityName(self.context.get().pipeline_service),
            )
            yield Either(right=pipeline_request)
            self.register_record(pipeline_request=pipeline_request)
        except Exception as exc:
            yield Either(
                left=StackTraceError(
                    name=pipeline_details.name,
                    error=f"Wild error ingesting pipeline {pipeline_details} - {exc}",
                    stackTrace=traceback.format_exc(),
                )
            )

    def yield_pipeline_lineage_details(
        self, pipeline_details: DBTJob
    ) -> Iterable[Either[AddLineageRequest]]:
        """
        Get lineage between pipeline and data sources
        """
        try:  # pylint: disable=too-many-nested-blocks
            if self.source_config.lineageInformation:
                pipeline_fqn = fqn.build(
                    metadata=self.metadata,
                    entity_type=Pipeline,
                    service_name=self.context.get().pipeline_service,
                    pipeline_name=self.context.get().pipeline,
                )

                pipeline_entity = self.metadata.get_by_name(
                    entity=Pipeline, fqn=pipeline_fqn
                )

                lineage_details = LineageDetails(
                    pipeline=EntityReference(
                        id=pipeline_entity.id.root, type="pipeline"
                    ),
                    source=LineageSource.PipelineLineage,
                )

                dbt_models = self.client.get_model_details(
                    job_id=pipeline_details.id, run_id=self.context.get().latest_run_id
                )

                for model in dbt_models or []:
                    for dbservicename in (
                        self.source_config.lineageInformation.dbServiceNames or []
                    ):
                        to_entity = self.metadata.get_by_name(
                            entity=Table,
                            fqn=fqn.build(
                                metadata=self.metadata,
                                entity_type=Table,
                                table_name=model.alias,
                                database_name=model.database,
                                schema_name=model.dbtschema,
                                service_name=dbservicename,
                            ),
                        )

                        if to_entity is None:
                            continue

                        for dest in model.parentsSources or []:
                            from_entity = self.metadata.get_by_name(
                                entity=Table,
                                fqn=fqn.build(
                                    metadata=self.metadata,
                                    entity_type=Table,
                                    table_name=dest.name,
                                    database_name=dest.database,
                                    schema_name=dest.dbtschema,
                                    service_name=dbservicename,
                                ),
                            )

                            if from_entity is None:
                                continue

                            yield Either(
                                right=AddLineageRequest(
                                    edge=EntitiesEdge(
                                        fromEntity=EntityReference(
                                            id=from_entity.id,
                                            type="table",
                                        ),
                                        toEntity=EntityReference(
                                            id=to_entity.id,
                                            type="table",
                                        ),
                                        lineageDetails=lineage_details,
                                    )
                                )
                            )

        except Exception as exc:
            yield Either(
                left=StackTraceError(
                    name=pipeline_details.name,
                    error=f"Wild error ingesting pipeline lineage {pipeline_details} - {exc}",
                    stackTrace=traceback.format_exc(),
                )
            )

    def get_pipelines_list(self) -> Iterable[DBTJob]:
        """
        Get List of all pipelines
        """
        try:
            yield from self.client.get_jobs()
        except Exception as exc:
            logger.debug(traceback.format_exc())
            logger.error(f"Failed to get pipeline list due to : {exc}")

    def get_pipeline_name(self, pipeline_details: DBTJob) -> str:
        """
        Get Pipeline Name
        """
        try:
            return pipeline_details.name
        except Exception as exc:
            logger.debug(traceback.format_exc())
            logger.error(f"Failed to get pipeline name due to : {exc}")

        return None

    def yield_pipeline_status(
        self, pipeline_details: DBTJob
    ) -> Iterable[Either[OMetaPipelineStatus]]:
        """
        Get Pipeline Status
        """
        try:
            task_status = [
                TaskStatus(
                    name=str(task.id),
                    executionStatus=STATUS_MAP.get(task.state, StatusType.Pending),
                    startTime=Timestamp(
                        datetime_to_ts(
                            datetime.strptime(task.started_at, "%Y-%m-%d %H:%M:%S.%f%z")
                            if task.started_at
                            else datetime.now()
                        )
                    ),
                    endTime=Timestamp(
                        datetime_to_ts(
                            datetime.strptime(
                                task.finished_at, "%Y-%m-%d %H:%M:%S.%f%z"
                            )
                            if task.finished_at
                            else datetime.now()
                        )
                    ),
                )
                for task in self.client.get_runs(job_id=int(pipeline_details.id)) or []
            ]

            pipeline_status = PipelineStatus(
                executionStatus=STATUS_MAP.get(
                    pipeline_details.state, StatusType.Pending
                ),
                taskStatus=task_status,
                timestamp=Timestamp(
                    datetime_to_ts(
                        datetime.strptime(
                            pipeline_details.created_at, "%Y-%m-%dT%H:%M:%S.%f%z"
                        )
                        if pipeline_details.created_at
                        else datetime.now()
                    )
                ),
            )

            pipeline_fqn = fqn.build(
                metadata=self.metadata,
                entity_type=Pipeline,
                service_name=self.context.get().pipeline_service,
                pipeline_name=self.context.get().pipeline,
            )
            yield Either(
                right=OMetaPipelineStatus(
                    pipeline_fqn=pipeline_fqn,
                    pipeline_status=pipeline_status,
                )
            )

        except Exception as exc:
            yield Either(
                left=StackTraceError(
                    name=pipeline_details.name,
                    error=f"Wild error ingesting pipeline status {pipeline_details} - {exc}",
                    stackTrace=traceback.format_exc(),
                )
            )
