from typing import TypeVar, Optional, List, Generic, Type, Iterable
from pydantic import BaseModel, Field, Extra
from functools import singledispatchmethod

from metadata.generated.schema.api.lineage.addLineage import AddLineageRequest
from metadata.ingestion.models.custom_properties import OMetaCustomProperties
from metadata.ingestion.models.ometa_classification import OMetaTagAndClassification
from metadata.ingestion.models.patch_request import PatchRequest
from metadata.ingestion.api.models import Either, Entity
from metadata.utils.source_hash import generate_source_hash
from metadata.ingestion.ometa.utils import model_str
from metadata.ingestion.models.topology.context_manager import TopologyContextManager
from metadata.ingestion.ometa.ometa_api import OpenMetadata
from metadata.utils.logger import ingestion_logger

logger = ingestion_logger()

T = TypeVar("T", bound=BaseModel)


class NodeStage(BaseModel, Generic[T]):
    """
    Handles the processing stages of each node.
    Each stage is equipped with a processing function
    and a context key, which will be updated at the
    source.
    """

    class Config:
        extra = Extra.forbid

    # Required fields to define the yielded entity type and the function processing it
    type_: Type[T] = Field(
        ..., description="Entity Type. E.g., DatabaseService, Database or Table"
    )
    processor: str = Field(
        ...,
        description="Has the producer results as an argument. Here is where filters happen. It will yield an Entity.",
    )

    # Topology behavior
    nullable: bool = Field(False, description="Flags if the yielded value can be null")
    must_return: bool = Field(
        False,
        description="The sink MUST return a value back after ack. Useful to validate if services are correct.",
    )
    overwrite: bool = Field(
        True,
        description="If we want to update existing data from OM. E.g., we don't want to overwrite services.",
    )
    consumer: Optional[List[str]] = Field(
        None,
        description="Stage dependency from parent nodes. Used to build the FQN of the processed Entity.",
    )

    # Context-related flags
    context: Optional[str] = Field(
        None, description="Context key storing stage state, if needed"
    )
    store_all_in_context: bool = Field(
        False, description="If we need to store all values being yielded in the context"
    )
    clear_context: bool = Field(
        False,
        description="If we need to clean the values in the context for each produced element",
    )
    store_fqn: bool = Field(
        False,
        description="If true, store the entity FQN in the context instead of just the name",
    )

    # Used to compute the fingerprint
    cache_entities: bool = Field(
        False,
        description="Cache all the entities which have use_cache set as True. Used for fingerprint comparison.",
    )
    use_cache: bool = Field(
        False,
        description="Enable this to get the entity from cached state in the context",
    )



class MissingExpectedEntityAckException(Exception):
    """
    After running the ack to the sink, we got no
    Entity back
    """


class NodeStageSinkRequestProcessor:
    @classmethod
    def sink_request(cls, stage: "NodeStage", entity_request: Either[BaseModel],  context: TopologyContextManager, metadata: OpenMetadata) -> Iterable[Either[Entity]]:
        """
        Validate that the entity was properly updated or retry if
        ack_sink is flagged.

        If we get the Entity back, update the context with it.

        :param stage: Node stage being processed
        :param entity_request: Request to pass
        :return: Entity generator
        """

        # Either use the received request or the acknowledged Entity
        entity = entity_request.right if entity_request else None

        if not stage.nullable and entity is None and entity_request.left is None:
            raise ValueError("Value unexpectedly None")

        if entity_request is not None:
            # Check that we properly received a Right response to process
            if entity_request.right is not None:
                # We need to acknowledge that the Entity has been properly sent to the server
                # to update the context
                if stage.context:
                    yield from cls.yield_and_update_context(
                        entity, stage=stage, entity_request=entity_request, context=context, metadata=metadata
                    )

                else:
                    yield entity_request

            else:
                # if entity_request.right is None, means that we have a Left. We yield the Either and
                # let the step take care of the
                yield entity_request

    @classmethod
    def create_patch_request(
            cls, original_entity: Entity, create_request: BaseModel
        ) -> PatchRequest:
            """
            Method to get the PatchRequest object
            To be overridden by the process if any custom logic is to be applied
            """
            return PatchRequest(
                original_entity=original_entity,
                new_entity=original_entity.copy(update=create_request.__dict__),
            )

    @classmethod
    def _is_force_overwrite_enabled(cls, metadata: OpenMetadata) -> bool:
        return metadata.config and metadata.config.forceEntityOverwriting

    @singledispatchmethod
    @classmethod
    def yield_and_update_context(
            cls,
            right: BaseModel,
            stage: "NodeStage",
            entity_request: Either[BaseModel],
            context: TopologyContextManager,
            metadata: OpenMetadata
        ) -> Iterable[Either[Entity]]:
            """
            Handle the process of yielding the request and validating
            that everything was properly updated.

            The default implementation is based on a get_by_name validation
            """
            entity = None
            entity_name = model_str(right.name)
            entity_fqn = context.get().fqn_from_stage(
                stage=stage, entity_name=entity_name
            )

            # If we don't want to write data in OM, we'll return what we fetch from the API.
            # This will be applicable for service entities since we do not want to overwrite the data
            same_fingerprint = False
            if not stage.overwrite and not cls._is_force_overwrite_enabled(metadata):
                entity = metadata.get_by_name(
                    entity=stage.type_,
                    fqn=entity_fqn,
                    fields=["*"],
                )
                if entity:
                    same_fingerprint = True

            create_entity_request_hash = None

            if hasattr(entity_request.right, "sourceHash"):
                create_entity_request_hash = generate_source_hash(
                    create_request=entity_request.right,
                )
                entity_request.right.sourceHash = create_entity_request_hash

            if entity is None and stage.use_cache:
                # check if we find the entity in the entities list
                entity_source_hash = context.cache[stage.type_].get(entity_fqn)
                if entity_source_hash:
                    # if the source hash is present, compare it with new hash
                    if entity_source_hash != create_entity_request_hash:
                        # the entity has changed, get the entity from server and make a patch request
                        entity = metadata.get_by_name(
                            entity=stage.type_,
                            fqn=entity_fqn,
                            fields=["*"],
                        )

                        # we return the entity for a patch update
                        if entity:
                            patch_entity = cls.create_patch_request(
                                original_entity=entity, create_request=entity_request.right
                            )
                            entity_request.right = patch_entity
                    else:
                        # nothing has changed on the source skip the API call
                        logger.debug(
                            f"No changes detected for {str(stage.type_.__name__)} '{entity_fqn}'"
                        )
                        same_fingerprint = True

            if not same_fingerprint:
                # We store the generated source hash and yield the request

                yield entity_request

            # We have ack the sink waiting for a response, but got nothing back
            if stage.must_return and entity is None:
                # we'll only check the get by name for entities like database service
                # without which we cannot proceed ahead in the ingestion
                tries = 3
                while not entity and tries > 0:
                    entity = metadata.get_by_name(
                        entity=stage.type_,
                        fqn=entity_fqn,
                        fields=["*"],  # Get all the available data from the Entity
                    )
                    tries -= 1

                if not entity:
                    # Safe access to Entity Request name
                    raise MissingExpectedEntityAckException(
                        f"Missing ack back from [{stage.type_.__name__}: {entity_fqn}] - "
                        "Possible causes are changes in the server Fernet key or mismatched JSON Schemas "
                        "for the service connection."
                    )

            context.get().update_context_name(stage=stage, right=right)

    @yield_and_update_context.register
    @classmethod
    def _(
        cls,
        right: AddLineageRequest,
        stage: "NodeStage",
        entity_request: Either[BaseModel],
        context: TopologyContextManager,
        metadata: OpenMetadata
    ) -> Iterable[Either[Entity]]:
        """
        Lineage Implementation for the context information.

        There is no simple (efficient) validation to make sure that this specific
        lineage has been properly drawn. We'll skip the process for now.
        """
        yield entity_request
        context.get().update_context_name(stage=stage, right=right.edge.fromEntity)

    @yield_and_update_context.register
    @classmethod
    def _(
        cls,
        right: OMetaTagAndClassification,
        stage: "NodeStage",
        entity_request: Either[BaseModel],
        context: TopologyContextManager,
        metadata: OpenMetadata
    ) -> Iterable[Either[Entity]]:
        """
        Tag implementation for the context information.

        We need the full OMetaTagAndClassification in the context
        to build the TagLabels during the ingestion. We need to bundle
        both CreateClassificationRequest and CreateTagRequest.
        """
        yield entity_request

        context.get().update_context_value(stage=stage, value=right)

    @yield_and_update_context.register
    @classmethod
    def _(
        cls,
        right: OMetaCustomProperties,
        stage: "NodeStage",
        entity_request: Either[BaseModel],
        context: TopologyContextManager,
        metadata: OpenMetadata
    ) -> Iterable[Either[Entity]]:
        """Custom Property implementation for the context information"""
        yield entity_request

        context.get().update_context_value(stage=stage, value=right)
