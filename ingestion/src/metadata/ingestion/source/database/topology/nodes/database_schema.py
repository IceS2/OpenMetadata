from typing import Iterable
from abc import ABC, abstractmethod
from metadata.ingestion.models.topology.node import TopologyNode
from metadata.ingestion.models.topology.node_stage import NodeStage
from metadata.ingestion.models.ometa_classification import OMetaTagAndClassification
from metadata.generated.schema.api.data.createDatabaseSchema import (
    CreateDatabaseSchemaRequest,
)
from metadata.generated.schema.entity.data.table import (
    Table,
)
from metadata.generated.schema.entity.data.storedProcedure import StoredProcedure

from metadata.ingestion.source.source import Source
from metadata.ingestion.api.models import Either
from metadata.ingestion.api.delete import delete_entity_from_source
from metadata.utils.logger import ingestion_logger

logger = ingestion_logger()

class DatabaseSchemaNode(TopologyNode, ABC):
    name = "databaseSchema"
    stages = [
        NodeStage(
            type_=OMetaTagAndClassification,
            context="tags",
            processor="yield_database_schema_tag_details",
            nullable=True,
            store_all_in_context=True,
        ),
        NodeStage(
            type_=DatabaseSchema,
            context="database_schema",
            processor="yield_database_schema",
            consumer=["database_service", "database"],
            cache_entities=True,
            use_cache=True,
        ),
    ]
    children=["table", "storedProcedure"]
    post_process=["mark_tables_as_deleted", "mark_stored_procedures_as_deleted"]
    threads=True

    @abstractmethod
    def produce(self, source: Source) -> Iterable[str]:
        pass

    @abstractmethod
    def yield_schema_tag(
        self, schema_name: str
    ) -> Iterable[Either[OMetaTagAndClassification]]:
        pass

    def yield_database_schema_tag_details(
        self, schema_name: str
    ) -> Iterable[Either[OMetaTagAndClassification]]:
        """
        From topology. To be run for each schema
        """
        if self.source_config.includeTags:
            yield from self.yield_schema_tag(schema_name)

    @abstractmethod
    def yield_database_schema(
        self, schema_name: str
    ) -> Iterable[Either[CreateDatabaseSchemaRequest]]:
        pass

    def mark_tables_as_deleted(self):
        """
        Use the current inspector to mark tables as deleted
        """
        if not self.context.get().__dict__.get("database"):
            raise ValueError(
                "No Database found in the context. We cannot run the table deletion."
            )

        if self.source_config.markDeletedTables:
            logger.info(
                f"Mark Deleted Tables set to True. Processing database [{self.context.get().database}]"
            )
            schema_fqn_list = self._get_filtered_schema_names(
                return_fqn=True, add_to_status=False
            )

            for schema_fqn in schema_fqn_list:
                yield from delete_entity_from_source(
                    metadata=self.metadata,
                    entity_type=Table,
                    entity_source_state=self.database_source_state,
                    mark_deleted_entity=self.source_config.markDeletedTables,
                    params={"database": schema_fqn},
                )

    def mark_stored_procedures_as_deleted(self):
        """
        Use the current inspector to mark Stored Procedures as deleted
        """
        if self.source_config.markDeletedStoredProcedures:
            logger.info(
                f"Mark Deleted Stored Procedures Processing database [{self.context.get().database}]"
            )

            schema_fqn_list = self._get_filtered_schema_names(
                return_fqn=True, add_to_status=False
            )

            for schema_fqn in schema_fqn_list:
                yield from delete_entity_from_source(
                    metadata=self.metadata,
                    entity_type=StoredProcedure,
                    entity_source_state=self.stored_procedure_source_state,
                    mark_deleted_entity=self.source_config.markDeletedStoredProcedures,
                    params={"databaseSchema": schema_fqn},
                )
