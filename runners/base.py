from typing import List

from configuration.pipeline import PipelineConfiguration
from pipeline.base import PipelineBase


class PipelineRunner:
    def __init__(self, configurations: PipelineConfiguration):
        self.configurations = configurations

    def run(self, pipeline_items: List[PipelineBase]):
        raise NotImplementedError()

