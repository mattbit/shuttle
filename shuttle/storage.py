import shutil
from pathlib import Path
from pymongo import MongoClient, ReturnDocument

class Storage:
    def __init__(self, config):
        self.client = MongoClient(config.get('host'), config.get('port'))
        self.db = self.client[config.get('database')]
        self.local_dir = Path(config.get('local_dir'))

    def find_one(self, collection, *args, **kwargs):
        return self.db[collection].find_one(*args, **kwargs)

    def find(self, collection, *args, **kwargs):
        return self.db[collection].find(*args, **kwargs)

    def find_one_and_update(self, collection, *args, **kwargs):
        return self.db[collection].find_one_and_update(*args, **kwargs)

    def update_one(self, collection, filter, query):
        return self.db[collection].update_one(filter, query)

    def insert_one(self, collection, *args, **kwargs):
        return self.db[collection].insert_one(*args, **kwargs)

    def drop(self, collection):
        return self.db[collection].drop()

    def bucket(self, id):
        return Bucket(id, self.local_dir.joinpath(id[:2], id))

    def buckets(self):
        return [Bucket(path.name, path)
                for path in self.local_dir.glob('??/*')]


class Bucket:
    def __init__(self, id, path):
        self.id = id
        self.path = path

    def new_file(self, name, overwrite=False):
        self.path.mkdir(parents=True, exist_ok=True)
        dst = self.path.joinpath(name)
        if not overwrite and dst.exists():
            raise Exception(f'The file `{dst}` already exists.')

        return dst

    def delete_file(self, name):
        self.path.joinpath(name).unlink()

    def drop(self):
        shutil.rmtree(self.path)
