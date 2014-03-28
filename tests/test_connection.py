# coding: utf-8

# $Id: $
import os
from unittest import TestCase

import asyncio
from asyncio_redis import Connection, NotConnectedError, Pool
import asyncio_redis

from tests.base import async
from async_redis.wrappers import ConnectionWrapper, PoolWrapper


class FDMixin:
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.pid = os.getpid()
        cls.poolsize = 8
        cls.conn_wait = 0.1
        cls.cmd_wait = 0.3

    def open_fd(self):
        return len(os.listdir('/proc/%s/fd/' % self.pid))


class ConnectionTestCase(FDMixin, TestCase):
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
            # закрываем случайное активное соединение
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
            # закрываем случайное активное соединение
            c.transport.close()
            # в close выполняется loop.call_soon(_reconnect)
            # который выполняется достаточно долго
            yield from asyncio.sleep(self.conn_wait)
            self.assertEqual(self.open_fd(), current)

    @async
    def testConnectionWrapperTimeoutStable(self):
        current = self.open_fd()
        for _ in range(10):
            c = yield from ConnectionWrapper.create(timeout=self.cmd_wait,
                                                    auto_reconnect=False)
            self.assertEqual(self.open_fd(), current + 1)
            with self.assertRaises(NotConnectedError):
                yield from c.blpop(["key1"], timeout=int(self.cmd_wait + 0.6))
            yield from asyncio.sleep(self.conn_wait)
            self.assertEqual(self.open_fd(), current)

    @async
    def testConnectionWrapperTimeoutReconnect(self):
        c = yield from ConnectionWrapper.create(timeout=self.cmd_wait,
                                                auto_reconnect=True)
        current = self.open_fd()
        for _ in range(10):
            with self.assertRaises(NotConnectedError):
                yield from c.blpop(["key1"], timeout=int(self.cmd_wait + 0.6))
            yield from asyncio.sleep(1.0)
            # реконнект
            self.assertEqual(self.open_fd(), current)

    @async
    def testPoolWrapperTimeoutClose(self):
        c = yield from PoolWrapper.create(timeout=self.cmd_wait,
                                          poolsize=self.poolsize,
                                          auto_reconnect=False)
        current = self.open_fd()
        for i in range(self.poolsize):
            with self.assertRaises(NotConnectedError):
                yield from c.blpop(["key1"], timeout=int(self.cmd_wait + 0.6))
            yield from asyncio.sleep(1.0)
            self.assertEqual(self.open_fd(), current - i - 1)

    @async
    def testPoolWrapperTimeoutReconnect(self):
        c = yield from PoolWrapper.create(timeout=self.cmd_wait,
                                          poolsize=self.poolsize,
                                          auto_reconnect=True)
        current = self.open_fd()
        for i in range(20):
            print(self.open_fd())
            with self.assertRaises(NotConnectedError):
                yield from c.blpop(["key1"], timeout=int(self.cmd_wait + 0.6))
            # в close выполняется loop.call_soon(_reconnect)
            # который выполняется достаточно долго
            yield from asyncio.sleep(1.0)
            # Число открытых соединений опускается до нуля, а затем
            # скачком открывается сразу poolsize соединений.

            # сразу реконнект на уровне одного соединения
            self.assertEqual(self.open_fd(), current)
            # self.assertEqual(self.open_fd(),
            #                  current - (i) % (self.poolsize + 1))


class RedisNotRunningTestCase(FDMixin, TestCase):

    @async
    def testConnectionStable(self):
        current = self.open_fd()
        for _ in range(20):
            with self.assertRaises(asyncio.futures.TimeoutError):
                yield from asyncio.wait_for(
                    Connection.create(auto_reconnect=False),
                    timeout=self.conn_wait)
            self.assertEqual(self.open_fd(), current)

    @async
    def testConnectionReconnect(self):
        current = self.open_fd()
        for _ in range(20):
            with self.assertRaises(asyncio.futures.TimeoutError):
                yield from asyncio.wait_for(
                    Connection.create(auto_reconnect=True),
                    timeout=self.conn_wait)
            self.assertEqual(self.open_fd(), current)

    @async
    def testPoolStable(self):
        current = self.open_fd()
        for _ in range(20):
            with self.assertRaises(asyncio.futures.TimeoutError):
                yield from asyncio.wait_for(
                    Pool.create(auto_reconnect=False, poolsize=self.poolsize),
                    timeout=self.conn_wait)
            self.assertEqual(self.open_fd(), current)

    @async
    def testPoolReconnect(self):
        current = self.open_fd()
        for _ in range(20):
            with self.assertRaises(asyncio.futures.TimeoutError):
                yield from asyncio.wait_for(
                    Pool.create(auto_reconnect=True, poolsize=self.poolsize),
                    timeout=self.conn_wait)
            self.assertEqual(self.open_fd(), current)

    @async
    def testConnectionWrapperStable(self):
        current = self.open_fd()
        for _ in range(20):
            with self.assertRaises(NotConnectedError):
                yield from ConnectionWrapper.create(
                    auto_reconnect=False,
                    timeout=self.conn_wait,
                    connect_timeout=self.conn_wait)
            self.assertEqual(self.open_fd(), current)

    @async
    def testConnectionWrapperReconnect(self):
        current = self.open_fd()
        for _ in range(20):
            with self.assertRaises(NotConnectedError):
                yield from ConnectionWrapper.create(
                    auto_reconnect=True,
                    timeout=self.conn_wait,
                    connect_timeout=self.conn_wait)
            self.assertEqual(self.open_fd(), current)

    @async
    def testPoolWrapperStable(self):
        current = self.open_fd()
        for _ in range(20):
            # создается пул с 0 доступных соединений
            c = yield from PoolWrapper.create(auto_reconnect=False,
                                          poolsize=self.poolsize,
                                          timeout=self.conn_wait,
                                          connection_timeout=self.conn_wait)
            self.assertEqual(self.open_fd(), current)
            self.assertEqual(c.connections_connected, 0)

    @async
    def testPoolWrapperReconnect(self):
        current = self.open_fd()
        for _ in range(20):
            # создается пул с 0 доступных соединений
            c = yield from PoolWrapper.create(auto_reconnect=True,
                                              poolsize=self.poolsize,
                                              timeout=self.conn_wait,
                                              connection_timeout=self.conn_wait)
            self.assertEqual(self.open_fd(), current)
            self.assertEqual(c.connections_connected, 0)


class RedisStoppedTestCase(FDMixin, TestCase):
    """ kill -STOP redis.pid """
    @async
    def testConnectionStable(self):
        current = self.open_fd()
        for _ in range(10):
            c = yield from Connection.create(auto_reconnect=False)
            self.assertEqual(self.open_fd(), current + 1)
            with self.assertRaises(asyncio.futures.TimeoutError):
                yield from asyncio.wait_for(c.get("key1"),
                                            timeout=self.cmd_wait)
            c.transport.close()
            yield from asyncio.sleep(self.conn_wait)

    @async
    def testConnectionReconnect(self):
        c = yield from Connection.create(auto_reconnect=True)
        current = self.open_fd()
        for _ in range(10):
            self.assertEqual(self.open_fd(), current)
            # Вот этот transport.close() еще надо вызвать, а он не вызывается
            # потому что c.get() выполняется бесконечно.
            asyncio.get_event_loop().call_later(self.cmd_wait,
                                                c.transport.close)
            with self.assertRaises(asyncio_redis.ConnectionLostError):
                yield from c.get("key1")
            yield from asyncio.sleep(1.0)

    @async
    def testPoolStable(self):
        c = yield from Pool.create(auto_reconnect=False, poolsize=self.poolsize)
        current = self.open_fd()
        for i in range(self.poolsize):
            print(self.open_fd())
            with self.assertRaises(asyncio.futures.TimeoutError):
                yield from asyncio.wait_for(c.get("key1"),
                                            timeout=self.cmd_wait)
            c.transport.close()
            yield from asyncio.sleep(1.0)
            self.assertEqual(self.open_fd(), current - i - 1)

    @async
    def testPoolReconnect(self):
        c = yield from Pool.create(auto_reconnect=True, poolsize=self.poolsize)
        current = self.open_fd()
        for _ in range(20):
            print(self.open_fd())
            # опять, нельзя на Task в состоянии CANCELLED сделать set_exception,
            # что происходит в c.transport.close
            # поэтому отмену делаем через закрытие по таймеру
            task = c.get("key1")
            protocol = task.gi_frame.f_locals['protocol_self']
            asyncio.get_event_loop().call_later(self.cmd_wait,
                                                protocol.transport.close)
            with self.assertRaises(asyncio_redis.ConnectionLostError):
                yield from task
            yield from asyncio.sleep(1.0)
            # реконнект в действии
            self.assertEqual(self.open_fd(), current)

    @async
    def testConnectionWrapperStable(self):
        current = self.open_fd()
        for _ in range(20):
            print(self.open_fd())
            with self.assertRaises(NotConnectedError):
                c = yield from ConnectionWrapper.create(
                    auto_reconnect=False,
                    timeout=self.conn_wait,
                    connect_timeout=self.conn_wait)
                yield from c.get('key1')
            yield from asyncio.sleep(1.0)
            self.assertEqual(self.open_fd(), current)

    @async
    def testConnectionWrapperReconnect(self):
        current = self.open_fd()
        c = yield from ConnectionWrapper.create(
                        auto_reconnect=True,
                        timeout=self.conn_wait,
                        connect_timeout=self.conn_wait)

        for _ in range(10):
            with self.assertRaises(NotConnectedError):
                yield from c.get('key1')
            yield from asyncio.sleep(1.0)
            # реконнект
            self.assertEqual(self.open_fd(), current + 1)

    @async
    def testPoolWrapperStable(self):
        c = yield from PoolWrapper.create(auto_reconnect=False,
                                      poolsize=self.poolsize,
                                      timeout=self.conn_wait,
                                      connection_timeout=self.conn_wait)
        current = self.open_fd()
        for i in range(20):
            print(self.open_fd())
            with self.assertRaises(NotConnectedError):
                yield from c.get('key1')
            yield from asyncio.sleep(2.0)
            #self.assertEqual(self.open_fd(), current - i)
            # self.assertEqual(c.connections_connected, 0)

    @async
    def testPoolWrapperReconnect(self):
        c = yield from PoolWrapper.create(auto_reconnect=True,
                                              poolsize=self.poolsize,
                                              timeout=self.conn_wait,
                                              connection_timeout=self.conn_wait)
        current = self.open_fd()
        for _ in range(20):
            print(self.open_fd())
            self.assertEqual(self.open_fd(), current)
            with self.assertRaises(NotConnectedError):
                yield from c.get('key1')
            yield from asyncio.sleep(1.0)
