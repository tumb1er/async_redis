# coding: utf-8

# $Id: $
import asyncio

import asyncio_redis

from async_redis import ConnectionWrapper


class Subscriber:
    """ Redis PUB/SUB client with connect timeout. """

    def __init__(self, channels, callback, host='localhost', port=6379,
                 timeout=1, pubsub_timeout=2):
        self.connection = None
        self.host = host
        self.port = port
        self.callback = callback
        self.channels = channels
        self.pubsub = None
        self.timeout = timeout
        self.pubsub_timeout = pubsub_timeout

    @asyncio.coroutine
    def _subscribe(self):
        """ Открывает соединение с Redis для обработки PUB/SUB.

        @return: флаг успешности подписки.
        """

        self.connection = yield from ConnectionWrapper.create(
            host=self.host, port=self.port, auto_reconnect=False)
        self.pubsub = yield from self.connection.start_subscribe()
        yield from self.pubsub.subscribe(self.channels)

    @asyncio.coroutine
    def _process_message(self):
        if not self.pubsub:
            raise asyncio_redis.NotConnectedError()
        reply = yield from self.pubsub.next_published()
        command = reply.value
        coro_or_none = self.callback(reply.channel, command)
        if asyncio.iscoroutine(coro_or_none):
            yield from coro_or_none

    def close(self):
        try:
            self.connection.close()
        finally:
            self.connection = None
            self.pubsub = None

    @asyncio.coroutine
    def connect(self):
        task = None
        try:
            task = asyncio.Task(self._subscribe())
            yield from asyncio.wait_for(
                task, timeout=self.pubsub_timeout)
        except asyncio.TimeoutError:
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
            # FIXME: no way to ping pubsub network status on today.
            self.close()
            connected = False
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
                yield from asyncio.sleep(self.pubsub_timeout)
