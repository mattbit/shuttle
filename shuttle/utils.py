class Doc(dict):
    def get(self, key):
        keys = key.split('.')
        value = self
        for k in keys:
            try:
                value = value[k]
            except KeyError:
                return None
        return value
