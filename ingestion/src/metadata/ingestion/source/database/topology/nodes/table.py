from typing import Iterable, Optional, Tuple
from abc import ABC, abstractmethod
from metadata.ingestion.models.topology.node import TopologyNode
from metadata.ingestion.models.topology.node_stage import NodeStage
from metadata.ingestion.models.ometa_classification import OMetaTagAndClassification
from metadata.generated.schema.entity.data.table import (
    Table,
)
from metadata.ingestion.models.life_cycle import OMetaLifeCycleData
from metadata.generated.schema.api.data.createTable import CreateTableRequest

from metadata.ingestion.source.source import Source
from metadata.ingestion.api.models import Either


class TableNode(TopologyNode, ABC):
    name = "table"
    stages = [
        NodeStage(
            type_=OMetaTagAndClassification,
            context="tags",
            processor="yield_table_tag_details",
            nullable=True,
            store_all_in_context=True,
        ),
        NodeStage(
            type_=Table,
            context="table",
            processor="yield_table",
            consumer=["database_service", "database", "database_schema"],
            use_cache=True,
        ),
        NodeStage(
            type_=OMetaLifeCycleData,
            processor="yield_life_cycle_data",
            nullable=True,
        ),
    ]

    @abstractmethod
    def produce(self, source: Source) -> Optional[Iterable[Tuple[str, str]]]:
        pass

    @abstractmethod
    def yield_table_tags(
        self, table_name_and_type: Tuple[str, str]
    ) -> Iterable[Either[OMetaTagAndClassification]]:
        pass

    def yield_table_tag_details(
        self, table_name_and_type: Tuple[str, str]
    ) -> Iterable[Either[OMetaTagAndClassification]]:
        if self.source_config.includeTags:
            yield from self.yield_table_tags(table_name_and_type)

    @abstractmethod
    def yield_table(
        self, table_name_and_type: Tuple[str, str]
    ) -> Iterable[Either[CreateTableRequest]]:
        pass
