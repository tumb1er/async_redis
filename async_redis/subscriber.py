# coding: utf-8

# $Id: $
from logging import getLogger
import asyncio
import asyncio_redis
from async_redis import errors


class ReconnectingSubscriber:
    """ Класс, отвечающий за подписку на сообщения из Redis и
    инициацию процесса перегрузки конфига."""
    logger = getLogger('%s.%s' % (__name__, "ReconnectingSubscriber"))

    def __init__(self, channels, callback, redis_factory, timeout=1,
                 pubsub_timeout=10):
        self.redis_factory = redis_factory
        self.callback = callback
        self.channels = channels
        self.pubsub = None
        self.exception_sent = False
        self.timeout = timeout
        self.pubsub_timeout = pubsub_timeout

    @asyncio.coroutine
    def subscribe(self):
        """ Открывает соединение с Redis для обработки PUB/SUB.

        @return: флаг успешности подписки.
        """

        try:
            self.logger.debug("connecting to redis")
            self.connection = yield from self.redis_factory
            self.logger.debug("init pubsub")
            self.pubsub = yield from self.connection.start_subscribe()
            self.logger.debug("subscribing")
            yield from self.pubsub.subscribe(self.channels)
            self.logger.debug("subscribed")
            self.exception_sent = False
            return True
        except Exception:
            if not self.exception_sent:
                self.logger.exception("can't subscribe")
            self.exception_sent = True
            return False

    @asyncio.coroutine
    def run_once(self):
        self.connected()
        reply = yield from self.pubsub.next_published()
        self.logger.debug("got event %s" % reply)
        command = reply.value
        coro_or_none = self.callback(command)
        if asyncio.iscoroutine(coro_or_none):
            yield from coro_or_none

    def close_pubsub(self):
        try:
            self.pubsub.protocol.transport.close()
        except:
            pass
        finally:
            self.pubsub = None

    def connect(self):
        task = None
        subscribed = False
        try:
            task = asyncio.Task(self.subscribe())
            subscribed = yield from asyncio.wait_for(
                task, timeout=self.pubsub_timeout)
        except asyncio.TimeoutError:
            if task:
                task.cancel()
        finally:
            self.close_pubsub()
        return subscribed

    def message_loop(self):
        connected = True
        try:
            # self.logger.debug("wait for message")

            yield from asyncio.wait_for(
                self.run_once(), timeout=self.pubsub_timeout)
        except asyncio.TimeoutError:
            try:
                if self.pubsub is None:
                    raise asyncio_redis.NotConnectedError()
                coro = self.pubsub.subscribe(self.channels)
                yield from asyncio.wait_for(
                    coro, timeout=self.pubsub_timeout)
            except errors.connection_errors:
                self.send_error("pubsub check failed")
                self.close_pubsub()
                connected = False
        except Exception:
            self.send_error("got exception in pubsub loop")
            connected = False
        return connected

    @asyncio.coroutine
    def __call__(self):
        """ Основной цикл работы обработчика.

        В бесконечном цикле:
        1) Соединяется с Redis и подписывается на PUB/SUB
        2) В еще одном бесконечном цикле обрабатывает полученные сообщения
        """
        while True:
            self.logger.debug("Start sub")
            subscribed = yield from self.connect()

            if not subscribed:
                yield from asyncio.sleep(self.pubsub_timeout)
                continue
            self.logger.debug("listening to channels")
            connected = True
            while connected:
                connected = yield from self.message_loop()
            self.logger.debug("got break in msg loop")

    def connected(self):
        self.exception_sent = False

    def send_error(self, message):
        if self.exception_sent:
            return
        self.exception_sent = True
        self.logger.exception(message)
