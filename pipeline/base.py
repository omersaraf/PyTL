import logging
from multiprocessing.pool import ThreadPool

from configuration.policy import ReadPolicy


class PipelineBase:
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.step = None
        self.policy = None

    def assign_step(self, step):
        self.step = step

    def set_policy(self, policy: ReadPolicy):
        self.policy = policy

    def before_run(self, last_state=None):
        pass

    def post_run(self):
        pass

    def apply_async(self, func, args):
        self.pool.apply(func, args)

    def init_thread_pool(self, thread_pool: ThreadPool):
        self.pool = thread_pool


class PipelineEndMessage:
    pass


END_MESSAGE = PipelineEndMessage()
