class Processor:

    def __init__(self, function):
        self.function = function

    def __call__(self, *args):
        return self.function(args)
