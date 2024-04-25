from abc import ABC, abstractmethod
from metadata.workflow.context import WorkflowContext

class Source(ABC):
    @property
    @abstractmethod
    def name(self) -> str:
        pass

    @abstractmethod
    def from_context(context: WorkflowContext):
        pass
