from typing import List, Iterable

from pipeline.base import END_MESSAGE
from writers.base import PipelineWriterBase
from queues.base import QueueBase


class PipelineProcessor(PipelineWriterBase):
    def process(self, input_queue: QueueBase, output_queue: QueueBase):
        for bulk in self._get_iterator(input_queue):
            for result in self._process(bulk):
                output_queue.push(result)

        output_queue.push(END_MESSAGE)

    def _process(self, items: List) -> Iterable:
        raise NotImplementedError()
