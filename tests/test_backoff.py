from schooling.backoff import MIN_TIMEOUT, SECONDS
from schooling.backoff import ExponentialBackoff
from schooling.backoff import LinearBackoff

def test_exponential_backoff():
    assert(ExponentialBackoff.timeout_ms(0) == MIN_TIMEOUT)
    max_1 = MIN_TIMEOUT + SECONDS
    assert(ExponentialBackoff.timeout_ms(1) < max_1)
    max_10 = MIN_TIMEOUT + (2 ** 10 - 1) * SECONDS
    assert(ExponentialBackoff.timeout_ms(10) < max_10)

def test_linear_backoff():
    assert(LinearBackoff.timeout_ms(0) == MIN_TIMEOUT)
    assert(LinearBackoff.timeout_ms(1) == MIN_TIMEOUT + SECONDS)
