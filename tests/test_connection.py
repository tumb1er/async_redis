# coding: utf-8

# $Id: $
import os
import socket
import subprocess
from unittest import TestCase

import asyncio
from asyncio_redis import Connection, NotConnectedError, Pool
import asyncio_redis
import resource

from tests.base import async
from async_redis.wrappers import ConnectionWrapper, PoolWrapper


class FDMixin:
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.pid = os.getpid()
        cls.poolsize = 8
        cls.conn_wait = 0.2
        cls.cmd_wait = 0.3
        cls.sleep = 2.0

    def open_fd(self):
        return len(os.listdir('/proc/%s/fd/' % self.pid))

    @classmethod
    def run_command(cls, *args):
        args = list(map(str, args))
        print(' '.join(args))
        subprocess.call(args)


class ConnectionTestCase(FDMixin, TestCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.run_command("sudo", "service", "redis-server", "restart")

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
            yield from asyncio.sleep(self.sleep)
            # реконнект
            print(self.open_fd())
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
            yield from asyncio.sleep(self.sleep)
            self.assertEqual(self.open_fd(), current - i - 1)

    @async
    def testPoolWrapperTimeoutReconnect(self):
        c = yield from PoolWrapper.create(timeout=self.cmd_wait,
                                          connect_timeout=self.conn_wait,
                                          poolsize=self.poolsize,
                                          auto_reconnect=True)
        current = self.open_fd()
        for i in range(20):
            print(self.open_fd())
            with self.assertRaises(NotConnectedError):
                yield from c.blpop(["key1"], timeout=int(self.cmd_wait + 0.6))
            # в close выполняется loop.call_soon(_reconnect)
            # который выполняется достаточно долго
            yield from asyncio.sleep(self.sleep)
            # Число открытых соединений опускается до нуля, а затем
            # скачком открывается сразу poolsize соединений.

            # сразу реконнект на уровне одного соединения
            self.assertEqual(self.open_fd(), current)
            # self.assertEqual(self.open_fd(),
            #                  current - (i) % (self.poolsize + 1))


class RedisNotRunningTestCase(FDMixin, TestCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.run_command("sudo", "service", "redis-server", "stop")

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
            print(self.open_fd())
            self.assertEqual(self.open_fd(), current)

    @async
    def testPoolWrapperStable(self):
        current = self.open_fd()
        for _ in range(20):
            # создается пул с 0 доступных соединений
            c = yield from PoolWrapper.create(auto_reconnect=False,
                                          poolsize=self.poolsize,
                                          timeout=self.conn_wait,
                                          connect_timeout=self.conn_wait)
            self.assertEqual(self.open_fd(), current)
            self.assertEqual(c.connections_connected, 0)

    @async
    def testPoolWrapperReconnect(self):
        current = self.open_fd()
        for i in range(200000):
            # создается пул с 0 доступных соединений
            c = yield from PoolWrapper.create(auto_reconnect=True,
                                              poolsize=self.poolsize,
                                              timeout=self.conn_wait,
                                              connect_timeout=self.conn_wait)
            yield from asyncio.sleep(self.sleep)
            # self.assertEqual(self.open_fd(), current)
            # self.assertEqual(c.connections_connected, 0)
            if i % 1000 == 0:
                print(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)


class RedisStoppedTestCase(FDMixin, TestCase):
    """ kill -STOP redis.pid """

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.sleep = 2.0
        cls.run_command("sudo", "service", "redis-server", "start")
        cls.run_command("sh", "-c", "sudo kill -STOP `sudo cat /var/run/redis/redis.pid`")

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()
        cls.run_command("sh", "-c", "sudo kill -CONT `sudo cat /var/run/redis/redis.pid`")

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
            yield from asyncio.sleep(self.sleep)

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
            yield from asyncio.sleep(self.sleep)
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
            yield from asyncio.sleep(self.sleep)
            # реконнект в действии
            self.assertEqual(self.open_fd(), current)

    @async
    def testConnectionWrapperStable(self):
        current = self.open_fd()
        for _ in range(20):
            print(self.open_fd())
            c = yield from ConnectionWrapper.create(
                    auto_reconnect=False,
                    timeout=self.conn_wait,
                    connect_timeout=1.0)
            with self.assertRaises(NotConnectedError):
                yield from c.get('key1')

            yield from asyncio.sleep(self.sleep)
            self.assertEqual(self.open_fd(), current)

    @async
    def testConnectionWrapperReconnect(self):
        current = self.open_fd()
        c = yield from ConnectionWrapper.create(
                        auto_reconnect=True,
                        timeout=self.conn_wait,
                        connect_timeout=2.0)
        # В теории мы законнектились, но не знаем что на той стороне все плохо
        self.assertEqual(self.open_fd(), current + 1)
        for _ in range(10):
            with self.assertRaises(NotConnectedError):
                yield from c.get('key1')
            yield from asyncio.sleep(self.sleep)
            # реконнект
            print(self.open_fd())
            self.assertEqual(self.open_fd(), current + 1)

    @async
    def testPoolWrapperStable(self):
        c = yield from PoolWrapper.create(auto_reconnect=False,
                                      poolsize=self.poolsize,
                                      timeout=self.conn_wait,
                                      connect_timeout=self.conn_wait)
        current = self.open_fd()
        for i in range(20):
            print(self.open_fd())
            with self.assertRaises(NotConnectedError):
                yield from c.get('key1')
            yield from asyncio.sleep(self.sleep)
            self.assertEqual(self.open_fd(), current - min(i, self.poolsize - 1) - 1)
            # self.assertEqual(c.connections_connected, 0)

    @async
    def testPoolWrapperReconnect(self):
        c = yield from PoolWrapper.create(auto_reconnect=True,
                                              poolsize=self.poolsize,
                                              timeout=self.conn_wait,
                                              connect_timeout=self.conn_wait)
        current = self.open_fd()
        for i in range(200000):
            #self.assertEqual(self.open_fd(), current)
            with self.assertRaises(NotConnectedError):
                yield from c.get('key1')
            yield from asyncio.sleep(self.sleep)
            if i % 1000 == 0:
                print(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)



class NoRouteTestCase(FDMixin, TestCase):
    """ no route to redis host """

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.host = '22.0.0.22'
        cls.sleep = 1.0

    @async
    def testConnectionStable(self):
        current = self.open_fd()
        for i in range(10):
            task = Connection.create(host=self.host,
                                                  auto_reconnect=False)
            with self.assertRaises(asyncio.futures.TimeoutError):
                yield from asyncio.wait_for(task, timeout=self.conn_wait)
            self.assertEqual(self.open_fd(), current + i + 1)
            yield from asyncio.sleep(self.conn_wait)

    @async
    def testConnectionReconnect(self):
        current = self.open_fd()
        for i in range(10):
            task = Connection.create(host=self.host, auto_reconnect=True)
            with self.assertRaises(asyncio.futures.TimeoutError):
                yield from asyncio.wait_for(task, timeout=self.conn_wait)
            self.assertEqual(self.open_fd(), current + i + 1)
            yield from asyncio.sleep(self.sleep)

    @async
    def testPoolStable(self):
        current = self.open_fd()
        for i in range(self.poolsize):
            print(self.open_fd())
            with self.assertRaises(asyncio.futures.TimeoutError):
                task = Pool.create(host=self.host, auto_reconnect=False,
                                          poolsize=self.poolsize)
                c = yield from asyncio.wait_for(task, timeout=self.conn_wait)
                yield from asyncio.wait_for(c.get("key1"),
                                            timeout=self.cmd_wait)
            yield from asyncio.sleep(self.sleep)
            self.assertEqual(self.open_fd(), current + i + 1)

    @async
    def testPoolReconnect(self):
        current = self.open_fd()
        for i in range(20):
            print(self.open_fd())
            task = Pool.create(host=self.host, auto_reconnect=True,
                               poolsize=self.poolsize)
            with self.assertRaises(asyncio.futures.TimeoutError):
                yield from asyncio.wait_for(task, timeout=self.conn_wait)
            yield from asyncio.sleep(self.sleep)
            # реконнект в действии - к сожалению, task.cancel не закроет
            # сокет, открытый где-то в недрах asyncio.
            self.assertEqual(self.open_fd(), current + i + 1)

    @async
    def testConnectionWrapperStable(self):
        current = self.open_fd()
        for i in range(20):
            print(self.open_fd())

            task = ConnectionWrapper.create(host=self.host,
                                          auto_reconnect=False,
                                          timeout=self.conn_wait,
                                          connect_timeout=self.conn_wait)
            with self.assertRaises(NotConnectedError):
                yield from task
                #yield from asyncio.wait_for(task, timeout=self.conn_wait)

            yield from asyncio.sleep(self.sleep)
            self.assertEqual(self.open_fd(), current)

    @async
    def testConnectionWrapperReconnect(self):
        current = self.open_fd()
        # В теории мы законнектились, но не знаем что на той стороне все плохо
        for i in range(10):
            task = ConnectionWrapper.create(
                        host=self.host,
                        auto_reconnect=True,
                        timeout=self.conn_wait,
                        connect_timeout=self.conn_wait)
            with self.assertRaises(NotConnectedError):
                yield from task
                # yield from asyncio.wait_for(task, timeout=self.conn_wait + 0.5)
            yield from asyncio.sleep(self.sleep)
            # реконнект
            print(self.open_fd())
            self.assertEqual(self.open_fd(), current)

    @async
    def testPoolWrapperStable(self):
        c = yield from PoolWrapper.create(host=self.host,
                                          auto_reconnect=False,
                                      poolsize=self.poolsize,
                                      timeout=self.conn_wait,
                                      connect_timeout=self.conn_wait)
        current = self.open_fd()
        for i in range(20):
            print(self.open_fd())
            with self.assertRaises(NotConnectedError):
                yield from c.get('key1')
            yield from asyncio.sleep(self.sleep)
            self.assertEqual(self.open_fd(), current)

    @async
    def testPoolWrapperReconnect(self):
        c = yield from PoolWrapper.create(host=self.host,
                                          auto_reconnect=True,
                                              poolsize=self.poolsize,
                                              timeout=self.conn_wait,
                                              connect_timeout=self.conn_wait)
        current = self.open_fd()
        for i in range(200000):
            # self.assertEqual(self.open_fd(), current)
            with self.assertRaises(NotConnectedError):
                yield from c.get('key1')
            yield from asyncio.sleep(self.sleep)
            if i % 1000 == 0:
                print(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)