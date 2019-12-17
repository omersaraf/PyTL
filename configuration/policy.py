class ReadPolicy:
    pass


class BulkReadPolicy(ReadPolicy):
    def __init__(self, bulk_size: int):
        self.bulk_size = bulk_size


class SingleItemReadPolicy(BulkReadPolicy):
    def __init__(self):
        super().__init__(1)


class BulkOrTimeoutReadPolicy(BulkReadPolicy):
    def __init__(self, bulk_size: int, timeout: float):
        super().__init__(bulk_size)
        self.timeout = timeout
