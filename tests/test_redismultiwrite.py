
import redis
import redismultiwrite as redismw
import unittest
import eventlet.debug

eventlet.debug.hub_exceptions(False)

class StrictRedisMock(object):
    class ConnectionPoolMock(object):
        def __init__(self, host):
            self.connection_kwargs = {'host': host}

    def __init__(self, id, broken=False):
        self.id = id
        self.broken = broken
        self.callstack = []
        self.connection_pool = self.ConnectionPoolMock(id)

    def get(self, key):
        if self.broken:
            raise redis.ConnectionError()
        self.callstack.append('get')
        return 'value' if key == 'good' else None

    def delete(self, key):
        if self.broken:
            raise redis.ConnectionError()
        self.callstack.append('delete')
        return key == 'good'

    def set(self, key, value):
        if self.broken:
            raise redis.ConnectionError()
        self.callstack.append('set')
        return True

    def setex(self, key, seconds, value):
        if self.broken:
            raise redis.ConnectionError()
        self.callstack.append('setex')
        return True

    def expire(self, key, seconds):
        if self.broken:
            raise redis.RedisError()
        self.callstack.append('expire')
        return key == 'good'

    def pipeline(self):
        if self.broken:
            raise redis.ConnectionError()
        self.callstack.append('pipeline')
        return self

    def execute(self):
        self.callstack.append('execute')
        return 


class RedisMultiWriteTest(unittest.TestCase):
    def setUp(self):
        self.local = StrictRedisMock('local')
        self.remote = [StrictRedisMock('remote1'), StrictRedisMock('remote2'),
                       StrictRedisMock('remote3', True)]
        self.redismw = redismw.RedisMultiWrite(self.local, self.remote)

    def test_other_methods(self):
        ret = self.redismw.get('good')
        self.assertEquals('value', ret)
        self.assertEquals(['get'], self.local.callstack)
        self.assertEquals([], self.remote[0].callstack)
        self.assertEquals([], self.remote[1].callstack)

    def test_missing_method(self):
        with self.assertRaises(AttributeError):
            self.redismw.test('stuff')
        self.assertEquals([], self.local.callstack)
        self.assertEquals([], self.remote[0].callstack)
        self.assertEquals([], self.remote[1].callstack)

    def test_local_only(self):
        local = StrictRedisMock('local')
        localonly = redismw.RedisMultiWrite(local)
        ret = localonly.delete_everywhere('good')
        self.assertTrue(ret)
        self.assertEquals(['delete'], local.callstack)

    def test_broken_local(self):
        broken = redismw.RedisMultiWrite(StrictRedisMock('broken', True),
                                         [StrictRedisMock('remote1')])
        with self.assertRaises(redismw.TooManyRetries) as cm:
            broken.delete_everywhere('good')
        self.assertEquals('broken', cm.exception.host)
        with self.assertRaises(redis.RedisError):
            broken.expire_everywhere('good', 10)

    def test_broken_local_only(self):
        broken = redismw.RedisMultiWrite(StrictRedisMock('broken', True))
        with self.assertRaises(redismw.TooManyRetries) as cm:
            broken.delete_everywhere('good')
        self.assertEquals('broken', cm.exception.host)

    def test_run_everywhere_get(self):
        ret = self.redismw.run_everywhere('get', ('good', ))
        self.assertEquals('value', ret)
        self.assertEquals(['get'], self.local.callstack)
        self.assertEquals(['get'], self.remote[0].callstack)
        self.assertEquals(['get'], self.remote[1].callstack)
        self.assertEquals([], self.remote[2].callstack)

    def test_delete_everywhere(self):
        ret = self.redismw.delete_everywhere('good')
        self.assertTrue(ret)
        self.assertEquals(['delete'], self.local.callstack)
        self.assertEquals(['delete'], self.remote[0].callstack)
        self.assertEquals(['delete'], self.remote[1].callstack)
        self.assertEquals([], self.remote[2].callstack)

    def test_delete_everywhere_missing_key(self):
        ret = self.redismw.delete_everywhere('bad')
        self.assertFalse(ret)

    def test_set_everywhere(self):
        ret = self.redismw.set_everywhere('good', 'value')
        self.assertTrue(ret)
        self.assertEquals(['set'], self.local.callstack)
        self.assertEquals(['set'], self.remote[0].callstack)
        self.assertEquals(['set'], self.remote[1].callstack)
        self.assertEquals([], self.remote[2].callstack)

    def test_setex_everywhere(self):
        ret = self.redismw.setex_everywhere('good', 10, 'value')
        self.assertTrue(ret)
        self.assertEquals(['setex'], self.local.callstack)
        self.assertEquals(['setex'], self.remote[0].callstack)
        self.assertEquals(['setex'], self.remote[1].callstack)
        self.assertEquals([], self.remote[2].callstack)

    def test_expire_everywhere(self):
        ret = self.redismw.expire_everywhere('good', 10)
        self.assertTrue(ret)
        self.assertEquals(['expire'], self.local.callstack)
        self.assertEquals(['expire'], self.remote[0].callstack)
        self.assertEquals(['expire'], self.remote[1].callstack)
        self.assertEquals([], self.remote[2].callstack)

    def test_expire_everywhere_missing_key(self):
        ret = self.redismw.expire_everywhere('bad', 10)
        self.assertFalse(ret)

    def test_pipeline_everywhere(self):
        commands = [('set', ('good', 'value')),
                    ('delete', ('good', ))]
        self.redismw.pipeline_everywhere(commands)
        expected = ['pipeline', 'set', 'delete', 'execute']
        self.assertEquals(expected, self.local.callstack)
        self.assertEquals(expected, self.remote[0].callstack)
        self.assertEquals(expected, self.remote[1].callstack)
        self.assertEquals([], self.remote[2].callstack)

# vim:et:fdm=marker:sts=4:sw=4:ts=4
