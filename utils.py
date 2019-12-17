from queue import Empty

from queues.base import QueueBase


def read_by_bulk_or_timeout(bulk_size: int, timeout: float, queue: QueueBase, termination):
    bulk = []
    while True:
        try:
            item = queue.pop(timeout)
            if item is termination:
                break
            bulk.append(item)

            if len(bulk) >= bulk_size:
                yield bulk
                bulk = []

        except Empty:
            if bulk:
                yield bulk
                bulk = []

    if bulk:
        yield bulk


def read_by_bulk(bulk_size: int, queue: QueueBase, termination):
    bulk = []
    while True:
        item = queue.pop()
        if item is termination:
            break

        bulk.append(item)
        if len(bulk) >= bulk_size:
            yield bulk
            bulk = []

    if bulk:
        yield bulk
