import time
from typing import List

from configuration.policy import BulkOrTimeoutReadPolicy, BulkReadPolicy
from pipeline.base import PipelineBase, END_MESSAGE
from queues.base import QueueBase
from utils import read_by_bulk_or_timeout, read_by_bulk


class PipelineWriterBase(PipelineBase):
    def __init__(self):
        super().__init__()
        self.policy = None

    @staticmethod
    def is_concurrent() -> bool:
        return False

    def _get_iterator(self, input_queue: QueueBase):
        if isinstance(self.policy, BulkOrTimeoutReadPolicy):
            return read_by_bulk_or_timeout(self.policy.bulk_size, self.policy.timeout, input_queue, END_MESSAGE)
        if isinstance(self.policy, BulkReadPolicy):
            return read_by_bulk(self.policy.bulk_size, input_queue, END_MESSAGE)
        raise Exception("Unknown read policy")


class PipelineWriter(PipelineWriterBase):
    def process(self, input_queue: QueueBase):
        count = 0
        for bulk in self._get_iterator(input_queue):
            self.logger.debug(f"Writing {len(bulk)} items")
            start_time = time.time()
            self._write(bulk)
            count += len(bulk)
            self.logger.info(f"Wrote total of {count} items")
            self.logger.debug(f"Wrote {len(bulk)} in {round(time.time() - start_time, 2)} seconds")

    def _write(self, items: List):
        raise NotImplementedError()
