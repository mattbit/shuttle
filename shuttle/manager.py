import uuid
from .storage import Storage
from .utils import Doc
from datetime import datetime
from blessings import Terminal


class Source:
    def __init__(self, name, action):
        self.name = name
        self.action = action

    def content(self, data=None):
        return self.action()


class TaskDefinition:
    def __init__(self, name, action, tags=None, priority=0):
        self.name = name
        self.action = action
        self.tags = tags if tags else []
        self.priority = priority


class Task:
    def __init__(self, manager, id, definition, config=None, status=None,
                 bucket=None, progress=0., priority=0):
        self._manager = manager
        self.id = id if id is not None else str(uuid.uuid4())
        self.definition = definition
        self.priority = priority
        self.config = Doc(config)
        self.bucket = bucket
        self.progress = progress
        self.status = status

    def __repr__(self):
        return f'<shuttle.manager.Task(name={self.definition.name})>'

    def progress(self, progress):
        self._manager.update_progress(self, progress)
        self.progress = progress

    def status(self, status):
        self._manager.update_status(self, status)
        self.status = status


class TaskQueue:
    def __init__(self, manager):
        self.manager = manager

    def _task_from_doc(self, doc):
        if not doc:
            return None

        task_def = self.manager.get_definition(doc['task'])
        task = Task(self.manager, doc['_id'], task_def,
                    status=doc['status'], config=doc['config'],
                    priority=doc['priority'])
        task.bucket = self.manager.storage.bucket(doc.get('bucket'))

        return task

    def _task_to_doc(self, task):
        return {
            '_id': task.id,
            'task': task.definition.name,
            'config': task.config,
            'priority': task.priority,
            'status': task.status,
            'bucket': task.bucket.id
        }

    def put(self, task):
        task.status = 'queued'
        doc = self._task_to_doc(task)
        doc['inserted'] = doc['updated'] = datetime.now()
        self.manager.storage.insert_one('tasks', doc)

    def get(self):
        doc = self.manager.storage.find_one('tasks',
                                            filter={'status': 'queued'},
                                            sort=[('priority', 1)])
        return self._task_from_doc(doc)

    def remove(self, task):
        self.manager.storage.remove_one({'_id': task.id})

    def status(self, task, status):
        task.status = status
        query = {'$set': {'status': status, 'updated': datetime.now()}}
        self.manager.storage.update_one('tasks', {'_id': task.id}, query)

    def done(self, task, status='done'):
        self.status(task, status)

    def all(self, *args, **kwargs):
        docs = self.manager.storage.find('tasks', *args, **kwargs)
        return [self._task_from_doc(doc) for doc in docs]

    def empty(self):
        self.manager.storage.drop('tasks')


class Manager:
    def __init__(self, config):
        self.storage = Storage(config)
        self._task_defs = dict()
        self._sources = dict()
        self.queue = TaskQueue(self)

    def register(self, obj):
        if isinstance(obj, TaskDefinition):
            return self.register_task(obj)

        if isinstance(obj, Source):
            return self.register_source(obj)

        obj_type = obj.__class__.__name__
        raise Exception(f'Cannot register object of type {obj_type}.')

    def register_task(self, task_def):
        self._task_defs[task_def.name] = task_def

    def register_source(self, source):
        self._sources[source.name] = source

    def get_definition(self, name):
        return self._task_defs.get(name)

    def get_source(self, id, data=None):
        source = self._sources[id]
        if source:
            return source.content(data)

    def enqueue(self, task_name, task_config, priority=None):
        task_def = self.get_definition(task_name)
        if priority is None:
            priority = task_def.priority
        task = Task(self, None, task_def,
                    config=task_config, priority=priority)
        task.bucket = self.storage.bucket(task.id)
        self.queue.put(task)

        return task

    def tasks(self, *args, **kwargs):
        return self.queue.all(*args, **kwargs)

    def process(self, errors=False):
        term = Terminal()
        task = self.queue.get()
        while task is not None:
            print(f'Processing task {task.definition.name}')
            print(f'Configuration {task.config}')
            try:
                self.queue.status(task, 'running')
                task.definition.action(self, task)
                self.queue.done(task)
                print('Task completed.\n')
            except Exception as err:
                if errors:
                    raise err
                self.queue.done(task, status='failed')
                print(f'Task failed with error: {err}.')

            # Next task
            task = self.queue.get()
