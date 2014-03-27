# coding: utf-8

# $Id: $
import concurrent.futures
import os
from unittest import TestCase
import asyncio
from asyncio_redis import Connection, NotConnectedError, Pool
from tests.base import async
from async_redis.wrappers import ConnectionWrapper, PoolWrapper


class ConnectionTestCase(TestCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.pid = os.getpid()
        cls.poolsize = 8
        cls.conn_wait = 0.1
        cls.cmd_wait = 0.1

    def open_fd(self):
        return len(os.listdir('/proc/%s/fd/' % self.pid))

    @async
    def testConnectionClose(self):
        """ Проверяем что при выключенном реконнекте явное закрытие соединения
        отпускает файловый дескриптор в asyncio_redis.Connection."""
        current = self.open_fd()
        for _ in range(10):
            c = yield from Connection.create(auto_reconnect=False)
            self.assertEqual(self.open_fd(), current + 1)
            yield from c.get("key1")
            c.transport.close()
            # в close выполняется loop.call_soon()
            yield from asyncio.sleep(self.conn_wait)
            self.assertEqual(self.open_fd(), current)
            print(self.open_fd())

    @async
    def testConnectionReconnect(self):
        """ Проверяет что при включенном реконнекте число файловых дескрипторов
        не растет."""
        c = yield from Connection.create(auto_reconnect=True)
        current = self.open_fd()
        for _ in range(10):
            yield from c.get("key1")
            c.transport.close()
            # в close выполняется loop.call_soon(_reconnect)
            # который выполняется достаточно долго
            yield from asyncio.sleep(self.conn_wait)
            self.assertEqual(self.open_fd(), current)
            print(self.open_fd())

    @async
    def testPoolClose(self):
        """ Проверяем что при выключенном реконнекте явное закрытие соединения
        отпускает файловый дескриптор в asyncio_redis.Connection."""
        c = yield from Pool.create(auto_reconnect=False, poolsize=self.poolsize)
        current = self.open_fd()
        for i in range(self.poolsize):
            yield from c.get("key1")
            c.transport.close()
            # в close выполняется loop.call_soon()
            yield from asyncio.sleep(self.conn_wait)
            self.assertEqual(self.open_fd(), current - i - 1)
            print(self.open_fd())

    @async
    def testPoolReconnect(self):
        """ Проверяет что при включенном реконнекте число файловых дескрипторов
        не растет."""
        c = yield from Pool.create(auto_reconnect=True, poolsize=self.poolsize)
        current = self.open_fd()
        for _ in range(20):
            yield from c.get("key1")
            c.transport.close()
            # в close выполняется loop.call_soon(_reconnect)
            # который выполняется достаточно долго
            yield from asyncio.sleep(self.conn_wait)
            self.assertEqual(self.open_fd(), current)
            print(self.open_fd())

    @async
    def testConnectionWrapperTimeoutStable(self):
        current = self.open_fd()
        for _ in range(10):
            c = yield from ConnectionWrapper.create(timeout=self.cmd_wait, auto_reconnect=False)
            self.assertEqual(self.open_fd(), current + 1)
            with self.assertRaises(NotConnectedError):
                yield from c.blpop(["key1"], timeout=int(self.cmd_wait + 0.6))
            yield from asyncio.sleep(self.conn_wait)
            self.assertEqual(self.open_fd(), current)
            print(self.open_fd())

    @async
    def testConnectionWrapperTimeoutReconnect(self):
        c = yield from ConnectionWrapper.create(timeout=self.cmd_wait, auto_reconnect=True)
        current = self.open_fd()
        for _ in range(10):
            with self.assertRaises(NotConnectedError):
                yield from c.blpop(["key1"], timeout=int(self.cmd_wait + 0.6))
            yield from asyncio.sleep(self.conn_wait)
            self.assertEqual(self.open_fd(), current - 1)
            print(self.open_fd())

    @async
    def testPoolWrapperTimeoutClose(self):
        c = yield from PoolWrapper.create(timeout=self.cmd_wait, poolsize=self.poolsize,
                                          auto_reconnect=False)
        current = self.open_fd()
        for i in range(self.poolsize):
            with self.assertRaises(NotConnectedError):
                yield from c.blpop(["key1"], timeout=int(self.cmd_wait + 0.6))
            yield from asyncio.sleep(self.conn_wait)
            self.assertEqual(self.open_fd(), current - i - 1)
            print(self.open_fd())

    @async
    def testPoolWrapperTimeoutReconnect(self):
        print(self.open_fd())
        c = yield from PoolWrapper.create(timeout=self.cmd_wait, poolsize=self.poolsize,
                                          auto_reconnect=True)
        current = self.open_fd()
        print("C=", current)
        for i in range(20):
            with self.assertRaises(NotConnectedError):
                yield from c.blpop(["key1"], timeout=int(self.cmd_wait + 0.6))
            # в close выполняется loop.call_soon(_reconnect)
            # который выполняется достаточно долго
            yield from asyncio.sleep(self.conn_wait)
            # Число открытых соединений опускается до нуля, а затем
            # скачком открывается сразу poolsize соединений.
            self.assertEqual(self.open_fd(), current - (i + 1) % (self.poolsize + 1))
            print(self.open_fd())


