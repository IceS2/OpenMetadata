from typing import Iterable, Any
from abc import ABC, abstractmethod
from metadata.ingestion.models.topology.node import TopologyNode
from metadata.ingestion.models.topology.node_stage import NodeStage
from metadata.generated.schema.api.data.createStoredProcedure import (
    CreateStoredProcedureRequest,
)
from metadata.generated.schema.entity.data.storedProcedure import StoredProcedure

from metadata.ingestion.source.source import Source
from metadata.ingestion.api.models import Either

class StoredProcedureNode(TopologyNode, ABC):
    name = "storedProcedure"
    stages = [
        NodeStage(
            type_=StoredProcedure,
            context="stored_procedures",
            processor="yield_stored_procedure",
            consumer=["database_service", "database", "database_schema"],
            store_all_in_context=True,
            store_fqn=True,
            use_cache=True,
        ),
    ]

    @abstractmethod
    def produce(self, source: Source) -> Iterable[Any]:
        pass

    @abstractmethod
    def yield_stored_procedure(
        self, stored_procedure: Any
    ) -> Iterable[Either[CreateStoredProcedureRequest]]:
        pass
