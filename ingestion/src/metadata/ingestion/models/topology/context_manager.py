import threading
from typing import Dict, Optional
from collections import defaultdict

from metadata.ingestion.models.topology.service import ServiceTopology
from metadata.ingestion.models.topology.context import TopologyContext

class TopologyContextManager:
    """Manages the Context held for different threads."""

    def __init__(self, topology: ServiceTopology):
        # The cache will have the shape {`child_stage.type_`: {`name`: `hash`}}
        self.cache = defaultdict(dict)
        # Due to our code strucutre, the first time the ContextManager is called will be within the MainThread.
        # We can leverage this to guarantee we keep track of the MainThread ID.
        self.main_thread = self.get_current_thread_id()
        self.contexts: Dict[int, TopologyContext] = {
            self.main_thread: TopologyContext.create(topology)
        }

        # Starts with the Multithreading disabled
        self.threads = 0

    def set_threads(self, threads: Optional[int]):
        self.threads = threads or 0

    def get_current_thread_id(self):
        return threading.get_ident()

    def get_global(self) -> TopologyContext:
        return self.contexts[self.main_thread]

    def get(self, thread_id: Optional[int] = None) -> TopologyContext:
        """Returns the TopologyContext of a given thread."""
        if thread_id:
            return self.contexts[thread_id]

        thread_id = self.get_current_thread_id()

        return self.contexts[thread_id]

    def pop(self, thread_id: Optional[int] = None):
        """Cleans the TopologyContext of a given thread in order to lower the Memory Profile."""
        if not thread_id:
            self.contexts.pop(self.get_current_thread_id())
        else:
            self.contexts.pop(thread_id)

    def copy_from(self, parent_thread_id: int):
        """Copies the TopologyContext from a given Thread to the new thread TopologyContext."""
        thread_id = self.get_current_thread_id()

        # If it does not exist yet, copies the Parent Context in order to have all context gathered until this point.
        self.contexts.setdefault(
            thread_id, self.contexts[parent_thread_id].copy(deep=True)
        )
