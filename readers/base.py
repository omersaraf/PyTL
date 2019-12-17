import time
from typing import Iterable

from pipeline.base import PipelineBase, END_MESSAGE
from queues.base import QueueBase


class PipelineReader(PipelineBase):
    def _create_iterator(self) -> Iterable:
        raise NotImplementedError()

    def process(self, output: QueueBase):
        last_time = None
        last_report = time.time()
        count = 0
        last_count = 0
        for item in self._create_iterator():
            if last_time is not None and time.time() - last_time > 0.5:
                self.logger.info(f"Read {count} items")
            output.push(item)
            count += 1
            last_count += 1
            last_time = time.time()
            if time.time() - last_report > 10:
                self.logger.info(f'{last_count / (time.time() - last_report)} files per second')
                last_report = time.time()
                last_count = 0
        output.push(END_MESSAGE)
