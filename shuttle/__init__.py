from .storage import Storage
from .manager import Manager, TaskDefinition, Source
from .database import session



class Workflow:

    def __init__(self, config):
        self.config = config
        self.manager = Manager(config)

    def task(self, action, name=None, tags=None, priority=0):
        """Register a new task definition."""
        if name is None:
            name = action.__name__

        task_def = TaskDefinition(name, action, tags, priority)
        self.manager.register(task_def)

        return action

    def source(self, action, name=None):
        """Register a new task source."""
        if name is None:
            name = action.__name__

        source = Source(name, action)
        self.manager.register(source)

        return action
