from typing import Iterable
from abc import ABC, abstractmethod
from metadata.ingestion.models.topology.node import TopologyNode
from metadata.ingestion.models.topology.node_stage import NodeStage
from metadata.ingestion.models.ometa_classification import OMetaTagAndClassification
from metadata.generated.schema.api.data.createDatabase import CreateDatabaseRequest

from metadata.ingestion.source.source import Source
from metadata.ingestion.api.models import Either

class DatabaseNode(TopologyNode, ABC):
    name = "database"
    stages = [
        NodeStage(
            type_=OMetaTagAndClassification,
            context="tags",
            processor="yield_database_tag_details",
            nullable=True,
            store_all_in_context=True,
        ),
        NodeStage(
            type_=Database,
            context="database",
            processor="yield_database",
            consumer=["database_service"],
            cache_entities=True,
            use_cache=True,
        ),
    ]
    children=["databaseSchema"]

    @abstractmethod
    def produce(self, source: Source) -> Iterable[str]:
        pass

    @abstractmethod
    def yield_database_tag(
        self, database_name: str
    ) -> Iterable[Either[OMetaTagAndClassification]]:
        """
        From topology. To be run for each schema
        """
        pass

    def yield_database_tag_details(
        self, database_name: str
    ) -> Iterable[Either[OMetaTagAndClassification]]:
        """
        From topology. To be run for each database
        """
        if self.source_config.includeTags:
            yield from self.yield_database_tag(database_name)

    @abstractmethod
    def yield_database(self, database_name: str) -> Iterable[Either[CreateDatabaseRequest]]:
        pass
