from configuration.policy import ReadPolicy
from queues.base import QueueFactory


class PipelineConfiguration:
    def __init__(self, queue_factory: QueueFactory, policy: ReadPolicy):
        self.policy = policy
        self.queue_factory = queue_factory
