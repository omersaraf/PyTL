from typing import List, Iterable

from processors.base import PipelineProcessor


class MockProcessor(PipelineProcessor):
    def _process(self, items: List) -> Iterable:
        yield from items[::-1]
