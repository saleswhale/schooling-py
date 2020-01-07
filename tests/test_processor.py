from schooling import Processor

def test_processor_init():
    assert(Processor(print).function == print)

def test_processor_function():
    def test_function(arg):
        return arg * 10
    processor = Processor(test_function)
    assert(processor(1) == test_function(1))