from abc import ABC, abstractmethod
from metadata.ingestion.models.topology.service import ServiceTopology
from metadata.ingestion.source.source import Source
from metadata.ingestion.source.database.topology.nodes.root import RootNode
from metadata.ingestion.source.database.topology.nodes.database import DatabaseNode
from metadata.ingestion.source.database.topology.nodes.database_schema import DatabaseSchemaNode
from metadata.ingestion.source.database.topology.nodes.table import TableNode
from metadata.ingestion.source.database.topology.nodes.stored_procedure import StoredProcedureNode

class DatabaseServiceTopology(ServiceTopology):
    """
    Defines the hierarchy in Database Services.
    service -> db -> schema -> table.

    We could have a topology validator. We can only consume
    data that has been produced by any parent node.
    """

    root: RootNode

    database: DatabaseNode

    databaseSchema: DatabaseSchemaNode

    table: TableNode

    storedProcedure: StoredProcedureNode


class DatabaseServiceSource(Source, ABC):  # pylint: disable=too-many-public-methods
    """
    Base class for Database Services.
    It implements the topology and context.
    """

    # source_config: DatabaseServiceMetadataPipeline
    # config: WorkflowSource
    # database_source_state: Set = set()
    # stored_procedure_source_state: Set = set()
    # # Big union of types we want to fetch dynamically
    # service_connection: DatabaseConnection.__fields__["config"].type_

    topology: DatabaseServiceTopology

    @property
    @abstractmethod
    def name(self) -> str:
        pass
