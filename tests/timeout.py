#!/usr/bin/env python

from redis import StrictRedis
from redismultiwrite import RedisMultiWrite, TooManyRetries

from eventlet.greenthread import sleep
from eventlet import monkey_patch


def main():
    monkey_patch()

    local = StrictRedis()
    remote = StrictRedis(port=6380, socket_timeout=0.2)
    rmw = RedisMultiWrite(local, [remote])

    print rmw.info_everywhere()
    sleep(2.0)


if __name__ == '__main__':
    main()


# vim:et:fdm=marker:sts=4:sw=4:ts=4
