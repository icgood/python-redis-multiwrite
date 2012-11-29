# vim: tabstop=4 shiftwidth=4 softtabstop=4

# Copyright 2012 Ian Good
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

"""The redismultiwrite module provides a wrapper to the python redis client,
exposing functions that will mimic multi-master behavior by performing some
write operations to several extra specified hosts before returning.

"""

import logging

import redis
from eventlet import greenpool
from eventlet import greenthread


class RedisMultiWriteError(redis.RedisError):
    """Base class for redismultiwrite errors."""
    pass


class TooManyRetries(redis.ConnectionError):
    """Exception thrown when an operation exceeds its allowed number of retries.
    
    """
    def __init__(self, exc, host):
        super(TooManyRetries, self).__init__(exc.message)
        self.host = host


class RedisMultiWrite(object):
    """Extends the functionality of the redis python client to perform some
    operations to a local redis instance as well as several remote instances
    for synchronization.
    
    """

    def __init__(self, local, remote=None, retries=3, log=None, pool_size=None):
        """Creates a new RedisMultiWrite object.

        :param local: A StrictRedis object representing a connection to the
                      local redis instance.
        :param remote: A list of StrictRedis objects representing connections to
                       remote redis instances. Default: empty list.
        :param retries: The number of times a write operation should be retried
                        on connection errors before failure. If this number is
                        exceeded, TooManyRetries is thrown with the error
                        message of the last attempt. Default: 3.
        :param log: The python-style log destination object. Defaults to the
                    standard destination of the `logging` module.
        :param pool_size: The size of the `GreenPool`. See the `eventlet`
                          library for details and default value.

        """
        self.local = local
        self.remote = remote or []
        self.retries = retries
        self.log = log or logging
        if pool_size:
            self.pool = greenpool.GreenPool(pool_size)
        else:
            self.pool = greenpool.GreenPool()

    def __getattr__(self, name):
        """Unless otherwise specified, methods on this object will be redirected
        to the local redis object given in the constructor.

        :param name: The method name passed to the local instance.

        """
        return getattr(self.local, name)

    def _wait_pile(self, pile):
        # Waits on a GreenPile to finish while ignoring thrown exceptions.
        while True:
            try:
                pile.next()
            except StopIteration:
                break
            except Exception:
                pass

    def _run_everywhere(self, op, args):
        # Performs an operation locally and then mimics it on remote clients.
        # This function only returns data for the local instance, but will
        # wait for all remote instances to finish (and ignores their success or
        # failure).
        if not self.remote:
            return self._attempt(self.local, op, args)
        pile = greenpool.GreenPile(self.pool)
        ret = self.pool.spawn(self._attempt, self.local, op, args)
        for server in self.remote:
            pile.spawn(self._attempt, server, op, args)
        try:
            return ret.wait()
        except TooManyRetries, e:
            self.log.error(e.message)
            raise
        finally:
            self._wait_pile(pile)

    def _attempt(self, conn, op, args):
        # This method is run for each redis connection in its own GreenThread.
        host = conn.connection_pool.connection_kwargs.get('host', '[Unknown]')
        last_connection_error = None
        for i in range(self.retries):
            try:
                return getattr(conn, op)(*args)
            except redis.ConnectionError, e:
                self.log.warn('Connectivity issue with '+host)
                last_connection_error = e
            except redis.RedisError, e:
                self.log.exception('Redis exception with '+host)
                greenthread.sleep(0)
                raise e
        greenthread.sleep(0)
        raise TooManyRetries(last_connection_error, host)

    def delete_everywhere(self, key):
        """Delete a key from the local redis instance and all remote redis
        instances. This operation is not atomic. The return value and/or
        exception thrown will only come from the local instance, but the method
        will not return until all child operations are finished.

        :param key: The redis key to delete.

        :returns: True if successful, False otherwise.
        :raises: TooManyRetries

        """
        return bool(self._run_everywhere('delete', (key, )))

    def set_everywhere(self, key, value):
        """Sets a key to a value on the local redis instance and all remote
        redis instances. This operation is not atomic. The return value and/or
        exception thrown will only come from the local instance, but the method
        will not return until all child operations are finished.

        :param key: The redis key to set.
        :param value: The new string value of the key.

        :returns: True if successful, False basically never.
        :raises: TooManyRetries

        """
        return self._run_everywhere('set', (key, value))

    def setex_everywhere(self, key, seconds, value):
        """Temporarily sets a key to a value on the local redis instance and all
        remote redis instances. This operation is not atomic. The return value
        and/or exception thrown will only come from the local instance, but the
        method will not return until all child operations are finished.

        :param key: The redis key to set.
        :param seconds: The number of seconds before key expires.
        :param value: The new string value of the key.

        :returns: True if successful, False basically never.
        :raises: TooManyRetries

        """
        return self._run_everywhere('setex', (key, seconds, value))

    def expire_everywhere(self, key, seconds):
        """Sets key expiration in seconds on the local redis instance and all
        remote redis instances. This operation is not atomic. The return value
        and/or exception thrown will only come from the local instance, but the
        method will not return until all child operations are finished.

        :param key: The redis key to set expiration.
        :param seconds: The number of seconds before key expires.

        :returns: True if successful, False basically never.
        :raises: TooManyRetries

        """

        return self._run_everywhere('expire', (key, seconds))

