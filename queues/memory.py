from collections import defaultdict
from typing import Optional, Any

from queues.base import QueueBase, QueueFactory


class MemoryQueue(QueueBase):
    def __init__(self):
        self.queue = []

    def push(self, item):
        self.queue.append(item)

    def pop(self, timeout=None) -> Optional[Any]:
        if self.queue:
            return self.queue.pop()
        return None


class MemoryQueueFactory(QueueFactory):
    def __init__(self, ):
        self.queues = defaultdict(lambda: MemoryQueue())

    def get_queue(self, step) -> QueueBase:
        return self.queues[step]
