from processors.base import PipelineProcessor
from readers.base import PipelineReader
from runners.base import PipelineRunner
from writers.base import PipelineWriter


class PipelineRunnerCreator:
    def __init__(self, pipeline_runner: PipelineRunner):
        self.pipeline_runner = pipeline_runner
        self.pipeline_items = []

    def __mod__(self, reader: PipelineReader):
        self.pipeline_items.append(reader)
        return self

    def __rshift__(self, processor: PipelineProcessor):
        self.pipeline_items.append(processor)
        return self

    def __and__(self, writer: PipelineWriter):
        self.pipeline_items.append(writer)
        self.pipeline_runner.run(self.pipeline_items)
        return self
