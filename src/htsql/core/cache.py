#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


from .context import context
import threading
import functools


class GeneralCache(object):

    def __init__(self):
        self.values = {}
        self.locks = {}
        self.cache_lock = threading.Lock()

    def lock(self, service):
        try:
            return self.locks[service]
        except KeyError:
            with self.cache_lock:
                if service not in self.locks:
                    self.locks[service] = threading.RLock()
            return self.locks[service]

    def set(self, key, value):
        with self.cache_lock:
            assert key not in self.values
            self.values[key] = value


def once(service):
    @functools.wraps(service)
    def wrapper(*args, **kwds):
        cache = context.app.htsql.cache
        key = (service.__module__, service.__name__) + args
        try:
            return cache.values[key]
        except KeyError:
            with cache.lock(service):
                if key not in cache.values:
                    value = service(*args, **kwds)
                    if key not in cache.values:
                        cache.set(key, value)
                    else:
                        assert value is cache.values[key]
            return cache.values[key]
    return wrapper


