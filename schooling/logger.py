import logging

class StreamLogger(logging.Logger):

    def __init__(self, name, verbose=False):
        super().__init__(name)
        log_level = logging.INFO if not verbose else logging.DEBUG
        self.setLevel(log_level)
        console = logging.StreamHandler()
        console.setLevel(log_level)
        log_format = '%(asctime)s | %(name)s [%(levelname)s] %(message)s'
        formatter = logging.Formatter(log_format)
        console.setFormatter(formatter)
        self.handlers = [console]
