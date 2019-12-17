from typing import List

from writers.base import PipelineWriter


class MockWriter(PipelineWriter):
    def before_run(self, last_state=None):
        self.count = 0

    def _write(self, items: List):
        print(items)
