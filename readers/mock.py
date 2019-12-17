from typing import Iterable

from readers.base import PipelineReader


class MockReader(PipelineReader):
    def __init__(self, iterator: Iterable):
        super().__init__()
        self.iterator = iterator

    def _create_iterator(self) -> Iterable:
        yield from self.iterator
