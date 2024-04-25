
import traceback
from typing import List, Optional, Type, Any
from pydantic import BaseModel, Extra, Field
from metadata.ingestion.api.models import Entity
from metadata.generated.schema.entity.data.database import Database
from metadata.generated.schema.entity.data.databaseSchema import DatabaseSchema
from metadata.generated.schema.entity.data.storedProcedure import StoredProcedure
from metadata.ingestion.models.topology.node_stage import NodeStage, NodeStageSinkRequestProcessor
from metadata.ingestion.models.topology.context_manager import TopologyContextManager
from metadata.ingestion.ometa.utils import model_str
from metadata.ingestion.ometa.ometa_api import OpenMetadata
from metadata.utils.logger import ingestion_logger

logger = ingestion_logger()

class TopologyNode(BaseModel):
    """
    Each node has a producer function, which will
    yield an Entity to be passed to the Sink. Afterwards,
    the producer function will update the Source context
    with the updated element from the OM API.
    """

    class Config:
        extra = Extra.forbid

    producer: str = Field(
        ...,
        description="Method name in the source called to generate the data. Does not accept input parameters",
    )
    stages: List[NodeStage] = Field(
        ...,
        description=(
            "List of functions to execute - in order - for each element produced by the producer. "
            "Each stage accepts the producer results as an argument"
        ),
    )
    children: Optional[List[str]] = Field(None, description="Nodes to execute next")
    post_process: Optional[List[str]] = Field(
        None, description="Method to be run after the node has been fully processed"
    )
    threads: bool = Field(
        False,
        description="Flag that defines if a node is open to MultiThreading processing.",
    )

    def process(self, node_entity: Any, child_nodes: List["TopologyNode"],context: TopologyContextManager, metadata: OpenMetadata):
        for stage in self.stages:
            stage_fn = getattr(self, stage.processor)
            for entity_request in stage_fn(node_entity, context, metadata) or []:
                try:
                    yield from NodeStageSinkRequestProcessor.sink_request(stage, entity_request, context, metadata)
                except ValueError as err:
                    logger.debug(traceback.format_exc())
                    logger.warning(
                        f"Unexpected value error when processing stage: [{stage}]: {err}"
                    )
            if stage.cache_entities:
                self._init_cache_dict(stage, child_nodes, context, metadata)


    def _init_cache_dict(
        self, stage: NodeStage, child_nodes: List["TopologyNode"], context: TopologyContextManager, metadata: OpenMetadata
    ) -> None:
        """
        Method to call the API to fill the entities cache.

        The cache will be part of the context
        """
        for child_node in child_nodes or []:
            for child_stage in child_node.stages or []:
                if child_stage.use_cache:
                    entity_fqn = context.get().fqn_from_stage(
                        stage=stage,
                        entity_name=context.get().__dict__[stage.context],
                    )

                    self._get_fqn_source_hash_dict(
                        parent_type=stage.type_,
                        child_type=child_stage.type_,
                        entity_fqn=entity_fqn,
                        context=context,
                        metadata=metadata
                    )

    def _get_fqn_source_hash_dict(
        self, parent_type: Type[Entity], child_type: Type[Entity], entity_fqn: str, context: TopologyContextManager, metadata: OpenMetadata
    ) -> None:
        """
        Get all the entities and store them as fqn:sourceHash in a dict
        """
        if parent_type in (Database, DatabaseSchema):
            if child_type == StoredProcedure:
                params = {"databaseSchema": entity_fqn}
            else:
                params = {"database": entity_fqn}
        else:
            params = {"service": entity_fqn}
        entities_list = metadata.list_all_entities(
            entity=child_type,
            params=params,
            fields=["sourceHash"],
        )
        for entity in entities_list:
            if entity.sourceHash:
                context.cache[child_type][
                    model_str(entity.fullyQualifiedName)
                ] = entity.sourceHash


def node_has_no_consumers(node: TopologyNode) -> bool:
    """
    Validate if a node has no consumers
    :param node:
    :return:
    """
    stage_consumers = [stage.consumer for stage in node.stages]
    return all(consumer is None for consumer in stage_consumers)
