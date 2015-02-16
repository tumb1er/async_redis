# coding: utf-8

# $Id: $
import asyncio
from logging import getLogger

import asyncio_redis

from async_redis import ConnectionWrapper


class Subscriber:
    """ Redis PUB/SUB client with connect timeout. """

    def __init__(self, channels, callback, host='localhost', port=6379,
                 timeout=1, pubsub_timeout=2, logger=None):
        self.connection = None
        self.host = host
        self.port = port
        self.callback = callback
        self.channels = channels
        self.pubsub = None
        self.timeout = timeout
        self.pubsub_timeout = pubsub_timeout
        self.logger = logger or getLogger(self.__class__.__name__)


    @asyncio.coroutine
    def _subscribe(self):
        """ Открывает соединение с Redis для обработки PUB/SUB.

        @return: флаг успешности подписки.
        """

        connection = None
        try:
            self.logger.debug("Start subscribe")
            connection = yield from ConnectionWrapper.create(
                host=self.host, port=self.port, auto_reconnect=False)
            self.logger.debug("Connection opened")
            pubsub = yield from connection.start_subscribe()
            self.logger.debug("Turn to pub/sub mode")
            yield from pubsub.subscribe(self.channels)
        except asyncio.CancelledError:
            self.logger.debug("Connect cancelled")
            if connection:
                connection.close()
            raise
        else:
            self.logger.debug("Switching pubsub")
            self.close()
            self.connection = connection
            self.pubsub = pubsub
        self.logger.debug("Subscribed to channels")

    @asyncio.coroutine
    def _process_message(self):
        if not self.pubsub:
            raise asyncio_redis.NotConnectedError()
        self.logger.debug("Wait for message")
        reply = yield from self.pubsub.next_published()
        self.logger.debug("Got message")
        command = reply.value
        try:
            coro_or_none = self.callback(reply.channel, command)
            if asyncio.iscoroutine(coro_or_none):
                yield from coro_or_none
        except Exception:
            self.logger.exception("Error while processing message")

    def close(self):
        self.logger.debug("Closing connection")
        try:
            if self.connection:
                self.connection.close()
        except Exception as e:
            self.logger.debug("Error closing connection: %s" % e)
        finally:
            self.connection = None
            self.pubsub = None

    @asyncio.coroutine
    def connect(self):
        task = None
        try:
            self.logger.debug("Start connect")
            task = asyncio.Task(self._subscribe())
            yield from asyncio.wait_for(
                task, timeout=self.pubsub_timeout)
        except asyncio.TimeoutError:
            self.logger.debug("Connect timeout")
            task.cancel()

            self.close()
            raise asyncio_redis.NotConnectedError()

    @asyncio.coroutine
    def _message_loop(self):
        connected = True
        try:
            yield from asyncio.wait_for(
                self._process_message(), timeout=self.pubsub_timeout)
        except asyncio.TimeoutError:
            connected = False
            yield from self.connect()
        return connected

    @asyncio.coroutine
    def run_forever(self):
        """ Основной цикл работы обработчика.

        В бесконечном цикле:
        1) Соединяется с Redis и подписывается на PUB/SUB
        2) В еще одном бесконечном цикле обрабатывает полученные сообщения
        """
        while True:
            try:
                yield from self.connect()
                while True:
                    yield from self._message_loop()
            except asyncio_redis.NotConnectedError:
                yield from asyncio.sleep(self.timeout)
