import logging
import uuid
from multiprocessing.pool import ThreadPool
from threading import Thread
from typing import List

from pipeline.base import PipelineBase
from processors.base import PipelineProcessor
from readers.base import PipelineReader
from runners.base import PipelineRunner
from writers.base import PipelineWriter

logging.basicConfig(level=logging.INFO, format='%(name)s\t[%(levelname)s]\t%(asctime)s\t\t%(message)s')

QUEUE_MAX_SIZE = 5000

STEP_FORMAT = '{token}_{index}'


class ThreadsPipelineRunner(PipelineRunner):
    def run(self, pipeline_items: List[PipelineBase], last_state=None):
        if not last_state:
            last_state = {}
        if len(pipeline_items) < 2:
            raise Exception("Pipeline must contains at least one reader and one writer")

        if not isinstance(pipeline_items[0], PipelineReader):
            raise Exception('First pipeline item must be Reader')

        if not isinstance(pipeline_items[-1], PipelineWriter):
            raise Exception('First pipeline item must be Writer')

        pool = ThreadPool()
        threads = []
        run_token = str(uuid.uuid4())

        for index, item in enumerate(pipeline_items):
            step = STEP_FORMAT.format(token=run_token, index=index)
            item.assign_step(step)
            item.set_policy(self.configurations.policy)
            item.init_thread_pool(pool)
            if isinstance(item, PipelineReader):
                item.before_run(last_state)
            else:

                item.before_run()

        prev_queue = None
        for index, item in enumerate(pipeline_items):
            step = STEP_FORMAT.format(token=run_token, index=index)
            queue = self.configurations.queue_factory.get_queue(step)
            if index == 0 and isinstance(item, PipelineReader):
                thread = Thread(target=item.process, args=(queue,))
            elif index == len(pipeline_items) - 1 and isinstance(item, PipelineWriter):
                thread = Thread(target=item.process, args=(prev_queue,))
            elif isinstance(item, PipelineProcessor):
                thread = Thread(target=item.process, args=(prev_queue, queue))
            else:
                raise Exception("All pipeline items should be either Reader/Writer/Processor")
            prev_queue = queue
            threads.append(thread)

        for thread in threads:
            thread.start()

        for thread in threads:
            thread.join()

        logging.info("Finished!")
