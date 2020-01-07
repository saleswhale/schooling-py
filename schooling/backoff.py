import random

SECONDS = 1000
MIN_TIMEOUT = 30 * SECONDS

class ExponentialBackoff:

    @staticmethod
    def timeout_ms(retries):
        return int(MIN_TIMEOUT + random.random() * (2 ** retries - 1) * SECONDS)

class LinearBackoff:

    @staticmethod
    def timeout_ms(retries):
        return int(MIN_TIMEOUT * retries * SECONDS)

