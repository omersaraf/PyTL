from collections import defaultdict
from queue import Queue
from typing import Any

from queues.base import QueueBase, QueueFactory


class ThreadQueue(QueueBase):
    def __init__(self, max_size: int):
        self.queue = Queue(max_size)

    def push(self, item: Any):
        self.queue.put(item)

    def pop(self, timeout=None) -> Any:
        return self.queue.get(timeout=timeout)


class ThreadQueueFactory(QueueFactory):
    def __init__(self, max_size: int = 0):
        self.queues = defaultdict(lambda: ThreadQueue(max_size))

    def get_queue(self, step) -> QueueBase:
        return self.queues[step]
