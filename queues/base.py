from typing import Any, Optional


class QueueBase:
    def push(self, item):
        raise NotImplementedError()

    def pop(self, timeout=None) -> Optional[Any]:
        raise NotImplementedError()


class QueueFactory:
    def get_queue(self, step) -> QueueBase:
        raise NotImplementedError()
