from typing import Iterable, Union
from abc import ABC, abstractmethod
from metadata.ingestion.models.topology.node import TopologyNode
from metadata.ingestion.models.topology.node_stage import NodeStage
from metadata.ingestion.models.topology.context_manager import TopologyContextManager

from metadata.generated.schema.api.lineage.addLineage import AddLineageRequest
from metadata.generated.schema.api.data.createQuery import CreateQueryRequest

from metadata.generated.schema.entity.services.databaseService import (
    DatabaseService,
)
from metadata.ingestion.source.source import Source
from metadata.generated.schema.metadataIngestion.workflow import (
    Source as WorkflowSource,
)
from metadata.generated.schema.api.services.createDatabaseService import (
    CreateDatabaseServiceRequest,
)
from metadata.ingestion.api.models import Either
from metadata.ingestion.ometa.ometa_api import OpenMetadata

class RootNode(TopologyNode, ABC):
    name = "root"
    stages = [
        NodeStage(
            type_=DatabaseService,
            context="database_service",
            processor="yield_create_request_database_service",
            overwrite=False,
            must_return=True,
            cache_entities=True,
        )
    ]
    children = ["database"]
    post_process=[
        "yield_view_lineage",
        "yield_procedure_lineage_and_queries",
        "yield_external_table_lineage"
    ]

    def produce(self, source: Source):
        yield source.config

    def yield_create_request_database_service(self, config: WorkflowSource, context: TopologyContextManager, metadata: OpenMetadata) -> Iterable[Either[CreateDatabaseServiceRequest]]:
        yield Either(
            right=metadata.get_create_service_from_source(
                entity=DatabaseService, config=config
            )
        )

    @abstractmethod
    def yield_view_lineage(self) -> Iterable[Either[AddLineageRequest]]:
        pass

    @abstractmethod
    def yield_procedure_lineage_and_queries(
        self,
    ) -> Iterable[Either[Union[AddLineageRequest, CreateQueryRequest]]]:
        pass

    @abstractmethod
    def yield_external_table_lineage(self) -> Iterable[Either[AddLineageRequest]]:
        pass
