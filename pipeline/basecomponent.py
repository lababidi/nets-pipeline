
class BaseComponent:

    def __init__(self,parameters):
        self.initialize( parameters)

    # should be overridden ...

    def initialize(self, parameters):
        pass


    # should be ovverridden ...
    # see Pipeline class for base event structure

    def process(self, events ):
        pass


