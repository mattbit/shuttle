# Shuttle 🚀

A simple task runner. **Work in progress, highly unstable.**

```python
from shuttle import Workflow

w = Workflow({'local_dir': '/data/storage'})

@w.source
def my_dataset():
  return ['a', 'lot', 'of', 'data']


@w.task
def my_task(manager, task):
  print(f'Running task {task.id}')
  data = manager.get_source('my_dataset')
  parameter = task.config.get('my.config.parameter')

  result = do_something_with(data, parameter)
  output = task.bucket.new_file('my_output.csv')
  with open(output, 'w') as out:
    out.write(result)

```
