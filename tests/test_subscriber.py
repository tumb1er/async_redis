# coding: utf-8

# $Id: $
from unittest import TestCase, skip
import asyncio
from asyncio_redis import NotConnectedError
from async_redis.subscriber import Subscriber
from tests.base import async
from tests.test_connection import FDMixin


class SubscriberTestCase(FDMixin, TestCase):

    @async
    def testSubscriberConnectAndClose(self):
        self.subscriber = Subscriber(['test_channel'], None)
        current = self.open_fd()

        yield from self.subscriber.connect()
        self.assertEqual(self.open_fd(), current + 1)

        self.subscriber.close()
        yield from asyncio.sleep(self.sleep)
        self.assertEqual(self.open_fd(), current)

    @async
    def testSubscriberReconnect(self):
        self.subscriber = Subscriber(['test_channel'], None,
                                     pubsub_timeout=self.sleep * 2)
        current = self.open_fd()

        task = asyncio.Task(self.subscriber.run_forever())

        yield from asyncio.sleep(self.sleep)

        self.assertEqual(self.open_fd(), current + 1)

        # закрываем соединение - надеемся, что оно переустановится
        self.subscriber.connection.close()

        yield from asyncio.sleep(self.sleep)
        self.assertEqual(self.open_fd(), current)

        # где-то тут срабатывает pubsub_timeout

        yield from asyncio.sleep(self.sleep)

        self.assertEqual(self.open_fd(), current)

        task.cancel()

    @async
    def testRedisNotRunning(self):
        self.run_command('sudo', 'service', 'redis-server', 'stop')
        self.subscriber = Subscriber(['test_channel'], None,
                                     pubsub_timeout=self.sleep * 2)
        current = self.open_fd()
        for _ in range(10):
            with self.assertRaises(NotConnectedError):
                yield from self.subscriber.connect()
            self.assertEqual(self.open_fd(), current)

    @async
    def testRedisStopped(self):
        self.run_command('sudo', 'service', 'redis-server', 'start')
        self.run_command("sh", "-c", "sudo kill -STOP `sudo cat /var/run/redis/redis.pid`")
        self.subscriber = Subscriber(['test_channel'], None,
                                     pubsub_timeout=self.sleep)
        current = self.open_fd()

        task = asyncio.Task(self.subscriber.run_forever())
        yield from asyncio.sleep(10 * self.sleep)
        self.assertEqual(self.open_fd(), current)
        task.cancel()

    @async
    def testNotRouteToRedis(self):
        self.subscriber = Subscriber(['test_channel'], None, host='22.0.0.22',
                                     pubsub_timeout=self.sleep)

        current = self.open_fd()

        task = asyncio.Task(self.subscriber.run_forever())
        yield from asyncio.sleep(10 * self.sleep)
        self.assertEqual(self.open_fd(), current)
        task.cancel()

    #@skip("manual test")
    @async
    def testManualTest(self):
        """ Just to run subscriber forever and test redis-server state handling.
        """
        def callback(ch, msg):
            print("R: %s F: %s" % (msg, ch))
        self.subscriber = Subscriber(['test_channel'], callback,
                                     pubsub_timeout=self.sleep)
        asyncio.Task(self.subscriber.run_forever())
        for _ in range(10000):
            yield from asyncio.sleep(self.sleep)
            print(self.open_fd())



