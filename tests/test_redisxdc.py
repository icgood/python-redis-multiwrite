
import redis
import redisxdc
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

class RedisXdcTest(unittest.TestCase):
    def setUp(self):
        self.local = StrictRedisMock('local')
        self.xdc = [StrictRedisMock('xdc1'), StrictRedisMock('xdc2'),
                    StrictRedisMock('xdc3', True)]
        self.redisxdc = redisxdc.RedisXdc(self.local, self.xdc)

    def test_other_methods(self):
        ret = self.redisxdc.get('good')
        self.assertEquals('value', ret)
        self.assertEquals(['get'], self.local.callstack)
        self.assertEquals([], self.xdc[0].callstack)
        self.assertEquals([], self.xdc[1].callstack)

    def test_missing_method(self):
        with self.assertRaises(AttributeError):
            self.redisxdc.test('stuff')
        self.assertEquals([], self.local.callstack)
        self.assertEquals([], self.xdc[0].callstack)
        self.assertEquals([], self.xdc[1].callstack)

    def test_local_only(self):
        local = StrictRedisMock('local')
        localonly = redisxdc.RedisXdc(local)
        ret = localonly.delete_everywhere('good')
        self.assertTrue(ret)
        self.assertEquals(['delete'], local.callstack)

    def test_broken_local(self):
        broken = redisxdc.RedisXdc(StrictRedisMock('broken', True),
                                   [StrictRedisMock('xdc1')])
        with self.assertRaises(redisxdc.TooManyRetries) as cm:
            broken.delete_everywhere('good')
        self.assertEquals('broken', cm.exception.host)
        with self.assertRaises(redis.RedisError):
            broken.expire_everywhere('good', 10)

    def test_broken_local_only(self):
        broken = redisxdc.RedisXdc(StrictRedisMock('broken', True))
        with self.assertRaises(redisxdc.TooManyRetries) as cm:
            broken.delete_everywhere('good')
        self.assertEquals('broken', cm.exception.host)

    def test_delete_everywhere(self):
        ret = self.redisxdc.delete_everywhere('good')
        self.assertTrue(ret)
        self.assertEquals(['delete'], self.local.callstack)
        self.assertEquals(['delete'], self.xdc[0].callstack)
        self.assertEquals(['delete'], self.xdc[1].callstack)
        self.assertEquals([], self.xdc[2].callstack)

    def test_delete_everywhere_missing_key(self):
        ret = self.redisxdc.delete_everywhere('bad')
        self.assertFalse(ret)

    def test_set_everywhere(self):
        ret = self.redisxdc.set_everywhere('good', 'value')
        self.assertTrue(ret)
        self.assertEquals(['set'], self.local.callstack)
        self.assertEquals(['set'], self.xdc[0].callstack)
        self.assertEquals(['set'], self.xdc[1].callstack)
        self.assertEquals([], self.xdc[2].callstack)

    def test_setex_everywhere(self):
        ret = self.redisxdc.setex_everywhere('good', 10, 'value')
        self.assertTrue(ret)
        self.assertEquals(['setex'], self.local.callstack)
        self.assertEquals(['setex'], self.xdc[0].callstack)
        self.assertEquals(['setex'], self.xdc[1].callstack)
        self.assertEquals([], self.xdc[2].callstack)

    def test_expire_everywhere(self):
        ret = self.redisxdc.expire_everywhere('good', 10)
        self.assertTrue(ret)
        self.assertEquals(['expire'], self.local.callstack)
        self.assertEquals(['expire'], self.xdc[0].callstack)
        self.assertEquals(['expire'], self.xdc[1].callstack)
        self.assertEquals([], self.xdc[2].callstack)

    def test_expire_everywhere_missing_key(self):
        ret = self.redisxdc.expire_everywhere('bad', 10)
        self.assertFalse(ret)

# vim:et:fdm=marker:sts=4:sw=4:ts=4
