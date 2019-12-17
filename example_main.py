from configuration.pipeline import PipelineConfiguration
from configuration.policy import BulkReadPolicy
from processors.mock import MockProcessor
from queues.threads import ThreadQueueFactory
from readers.mock import MockReader
from runners.helpers import PipelineRunnerCreator
from runners.threads import ThreadsPipelineRunner
from writers.mock import MockWriter

if __name__ == "__main__":
    configuration = PipelineConfiguration(ThreadQueueFactory(), BulkReadPolicy(3))
    creator = PipelineRunnerCreator(ThreadsPipelineRunner(configuration))
    creator % MockReader(['Hello', 'World', 'From', 'Pipeline']) >> MockProcessor() & MockWriter()
