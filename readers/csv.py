import csv
import sys
from typing import Dict, Iterable

from readers.base import PipelineReader

csv.field_size_limit(sys.maxsize)


class CsvPipelineReader(PipelineReader):
    def __init__(self, path: str, row_mapping: Dict[int, str], separation=',', use_csv_reader: bool = False):
        super().__init__()
        self.use_csv_reader = use_csv_reader
        self.separation = separation
        self.row_mapping = row_mapping
        self.path = path
        self.last_index = 0

    def before_run(self, last_state: dict = None):
        self.last_index = last_state.get('index', 0)

    def _create_reader(self, reader):
        if self.use_csv_reader:
            yield from csv.reader(reader, delimiter=self.separation)
        else:
            for row in reader:
                yield row.split(self.separation)

    def _create_iterator(self) -> Iterable:
        cnt = 0
        skipped = 0
        with open(self.path, 'r', encoding='utf-8', errors='ignore', newline='') as reader:
            for row in self._create_reader(reader):
                cnt += 1
                if cnt <= self.last_index:
                    continue

                if all([not not row[index].strip() for index in self.row_mapping.keys()]):
                    yield {key: row[index].strip() for index, key in self.row_mapping.items()}
                else:
                    skipped += 1
                if cnt % 100000 == 0:
                    self.logger.debug(f'Count = {cnt}, Skipped = {skipped}')