from typing import Any
import queue

class Queue:
    """Small Queue wrapper"""

    def __init__(self):
        self._queue = queue.Queue()

    def has_tasks(self) -> bool:
        """Checks that the Queue is not Empty."""
        return not self._queue.empty()

    def process(self) -> Any:
        """Yields all the items currently on the Queue."""
        while True:
            try:
                item = self._queue.get_nowait()
                yield item
                self._queue.task_done()
            except queue.Empty:
                break

    def put(self, item: Any):
        """Puts new item in the Queue."""
        self._queue.put(item)
