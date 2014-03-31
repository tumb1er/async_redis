# coding: utf-8

# $Id: $
from unittest import TestCase

import asyncio

from async_redis import Connections
from tests.base import async
from tests.test_connection import FDMixin


class ConnectionCacheTestCase(FDMixin, TestCase):
    """ Проверяет корректность работы кэша коннектов."""

    def setUp(self):
        super().setUp()
        self.cache = Connections({
            'default': {
                'host': '192.168.144.154',
                'poolsize': self.poolsize,
                'timeout': self.conn_wait,
                'connect_timeout': self.conn_wait,
                'auto_reconnect': False,
            },
            'single': {
                'host': '192.168.144.154',
                'timeout': self.conn_wait,
                'connect_timeout': self.conn_wait,
                'auto_reconnect': False,
            }
        })

    @async
    def testParallelConnectPool(self):
        current = self.open_fd()
        loop = asyncio.get_event_loop()
        loop.call_later(0.5, asyncio.async,
                        self.cache.get_connection())
        loop.call_soon(asyncio.async, self.cache.get_connection())
        yield from asyncio.sleep(self.sleep)
        self.assertEqual(self.open_fd(), current + self.poolsize)

    @async
    def testCloseConnectionPool(self):
        current = self.open_fd()
        conn = yield from self.cache.get_connection()
        self.assertEqual(self.open_fd(), current + self.poolsize)
        self.cache.close_connection()
        yield from asyncio.sleep(self.sleep)
        self.assertEqual(self.open_fd(), current)

    @async
    def testParallelConnectSingle(self):
        current = self.open_fd()
        loop = asyncio.get_event_loop()
        loop.call_later(0.5, asyncio.async,
                        self.cache.get_connection('single'))
        loop.call_soon(asyncio.async, self.cache.get_connection('single'))
        yield from asyncio.sleep(self.sleep)
        self.assertEqual(self.open_fd(), current + 1)

    @async
    def testCloseConnectionSingle(self):
        current = self.open_fd()
        conn = yield from self.cache.get_connection('single')
        self.assertEqual(self.open_fd(), current + 1)
        self.cache.close_connection('single')
        yield from asyncio.sleep(self.sleep)
        self.assertEqual(self.open_fd(), current)
