""" Classes to handle connections to Memcached """
import logging
from codecs import decode
import backoff as backoff
from pymemcache import MemcacheServerError
from pymemcache.client import base

# maximum number of calls to make to the target function before giving up
MAX_RETRY_ATTEMPTS = 5

# maximum amount of total time in seconds that can elapse before giving up
MEMCACHED_TIMEOUT = 10


class MemCacheConnections:
    """ Handles client connections to multiple instances """
    clients = {}

    @classmethod
    def get_client(cls, addr, new=False):
        """ Creates connection if it does not exist """
        if new or addr not in cls.clients:
            logging.info("New connection to Memcached instance %s", addr)
            cls.clients[addr] = base.Client(addr)
        else:
            logging.debug("Existing connection selected %s", addr)
        return cls.clients[addr]


@backoff.on_exception(
    backoff.expo,
    (ConnectionRefusedError, MemcacheServerError),
    max_tries=MAX_RETRY_ATTEMPTS,
    max_time=MEMCACHED_TIMEOUT,
    jitter=None)
def memc_get(addr, *args, **kwargs):
    """ Get value for given key from Memcached """
    value = MemCacheConnections().get_client(addr).get(*args, **kwargs)
    return value if value is None else decode(value)


@backoff.on_exception(
    backoff.expo,
    (ConnectionRefusedError, MemcacheServerError),
    max_tries=MAX_RETRY_ATTEMPTS,
    max_time=MEMCACHED_TIMEOUT,
    jitter=None)
def memc_set(addr, *args, **kwargs):
    """ Set value for given key into Memcached """
    return MemCacheConnections().get_client(addr).set(*args, **kwargs)


@backoff.on_exception(
    backoff.expo,
    (ConnectionRefusedError, MemcacheServerError),
    max_tries=MAX_RETRY_ATTEMPTS,
    max_time=MEMCACHED_TIMEOUT,
    jitter=None)
def memc_set_multi(addr, content):
    """ Set values for given keys into Memcached """
    return MemCacheConnections().get_client(addr).set_multi(content)
