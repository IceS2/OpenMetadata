from typing import Any, Optional, TypeVar
from pydantic import BaseModel, Extra, create_model
from functools import singledispatchmethod

from metadata.ingestion.models.topology.node import NodeStage
from metadata.ingestion.moels.topology.service import ServiceTopology, get_topology_nodes
from metadata.generated.schema.api.data.createStoredProcedure import (
    CreateStoredProcedureRequest,
)
from metadata.generated.schema.entity.data.storedProcedure import StoredProcedure
from metadata.ingestion.ometa.utils import model_str
from metadata.utils import fqn

C = TypeVar("C", bound=BaseModel)

class TopologyContext(BaseModel):
    """
    Bounds all topology contexts
    """

    class Config:
        extra = Extra.allow

    def __repr__(self):
        ctx = {key: value.name.__root__ for key, value in self.__dict__.items()}
        return f"TopologyContext({ctx})"

    @classmethod
    def create(cls, topology: ServiceTopology) -> "TopologyContext":
        """
        Dynamically build a context based on the topology nodes.

        Builds a Pydantic BaseModel class.

        :param topology: ServiceTopology
        :return: TopologyContext
        """
        nodes = get_topology_nodes(topology)
        ctx_fields = {
            stage.context: (Optional[stage.type_], None)
            for node in nodes
            for stage in node.stages
            if stage.context
        }
        return create_model(
            "GeneratedContext", **ctx_fields, __base__=TopologyContext
        )()

    def upsert(self, key: str, value: Any) -> None:
        """
        Update the key of the context with the given value
        :param key: element to update from the source context
        :param value: value to use for the update
        """
        self.__dict__[key] = value

    def append(self, key: str, value: Any) -> None:
        """
        Update the key of the context with the given value
        :param key: element to update from the source context
        :param value: value to use for the update
        """
        if self.__dict__.get(key):
            self.__dict__[key].append(value)
        else:
            self.__dict__[key] = [value]

    def clear_stage(self, stage: NodeStage) -> None:
        """
        Clear the available context
        :param stage: Update stage context to the default values
        """
        self.__dict__[stage.context] = None

    def fqn_from_stage(self, stage: NodeStage, entity_name: str) -> str:
        """
        Read the context
        :param stage: Topology node being processed
        :param entity_name: name being stored
        :return: Entity FQN derived from context
        """
        context_names = [
            self.__dict__[dependency]
            for dependency in stage.consumer or []  # root nodes do not have consumers
        ]
        return fqn._build(  # pylint: disable=protected-access
            *context_names, entity_name
        )

    def update_context_name(self, stage: NodeStage, right: C) -> None:
        """
        Append or update context

        We'll store the entity name or FQN in the topology context.
        If we store the name, the FQN will be built in the source itself when needed.
        """

        if stage.store_fqn:
            new_context = self._build_new_context_fqn(right)
        else:
            new_context = model_str(right.name)

        self.update_context_value(stage=stage, value=new_context)

    def update_context_value(self, stage: NodeStage, value: Any) -> None:
        if stage.context and not stage.store_all_in_context:
            self.upsert(key=stage.context, value=value)
        if stage.context and stage.store_all_in_context:
            self.append(key=stage.context, value=value)

    @singledispatchmethod
    def _build_new_context_fqn(self, right: C) -> str:
        """Build context fqn string"""
        raise NotImplementedError(f"Missing implementation for [{type(C)}]")

    @_build_new_context_fqn.register
    def _(self, right: CreateStoredProcedureRequest) -> str:
        """
        Implement FQN context building for Stored Procedures.

        We process the Stored Procedures lineage at the very end of the service. If we
        just store the SP name, we lose the information of which db/schema the SP belongs to.
        """

        return fqn.build(
            metadata=None,
            entity_type=StoredProcedure,
            service_name=self.__dict__["database_service"],
            database_name=self.__dict__["database"],
            schema_name=self.__dict__["database_schema"],
            procedure_name=right.name.__root__,
        )
