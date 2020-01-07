import logging

import pytest

from schooling.logger import StreamLogger

@pytest.fixture
def stream_logger():
    return StreamLogger('test_logger')

@pytest.fixture
def verbose_logger():
    return StreamLogger('verbose_logger')

def test_logger_init(stream_logger):
    assert(stream_logger.name == 'test_logger')
    assert(stream_logger.isEnabledFor(logging.INFO))
    assert(not stream_logger.isEnabledFor(logging.DEBUG))
    assert(len(stream_logger.handlers) == 1)

def test_verbose_logger(verbose_logger):
    assert(not verbose_logger.isEnabledFor(logging.DEBUG))
