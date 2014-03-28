# coding: utf-8

import functools
import logging
import asyncio

from asyncio_redis.exceptions import NotConnectedError
from asyncio_redis.protocol import _all_commands, RedisProtocol
from asyncio_redis import Connection, Pool


def timeout_aware_pool(cls):
    """ Декоратор класса, добавляющий таймауты для операций Redis
    для класса PoolWrapper.
    """

    def _timeout_aware_decorator(cmd):
        @asyncio.coroutine
        @functools.wraps(cmd)
        def wrapper(pool, *args, **kwargs):
            # восстанавливаем число соединений до poolsize
            yield from pool._reconnect()
            # выбираем свободное соединение
            connection = pool._get_free_connection()
            if connection is None:
                # нет свободных, вызываем процесс очистки соединений и
                # возбуждаем ошибку
                pool._cleanup_connections()
                raise NotConnectedError()

            # вызываем команду редиса
            task = cmd(connection.protocol, *args, **kwargs)
            try:
                # при необходимости, оборачиваем в wait_for
                timeout = pool._timeout
                if timeout is None:
                    result = yield from task
                else:
                    result = yield from asyncio.wait_for(
                        task, pool._timeout, loop=pool._loop)
            except (asyncio.futures.TimeoutError, NotConnectedError):
                # произошел таймаут, ошибка коннекта:
                # закрываем соединение на транспортном уровне
                try:
                    if connection.protocol.transport is not None:
                        connection.protocol.transport.close()
                except Exception:
                    logging.exception(
                        "Error while handling redis operation timeout")
                raise NotConnectedError()
            return result

        return wrapper

    # Все команды редиса оборачиваем в декоратор.
    for method in _all_commands:
        setattr(cls, method,
                _timeout_aware_decorator(getattr(RedisProtocol, method)))
    return cls


def timeout_aware_conn(cls):
    """ Декоратор, добавляющий таймауты для операций Redis
        для реализации Connection."""

    def _timeout_aware_connection(cmd):

        @asyncio.coroutine
        @functools.wraps(cmd)
        def wrapper(connection, *args, **kwargs):
            # Выполняем команду протокола Redis
            protocol = connection.protocol
            task = cmd(protocol, *args, **kwargs)
            try:
                # При необходимости оборачиваем в wait_for
                timeout = connection._timeout
                if timeout is None:
                    result = yield from task
                else:
                    result = yield from asyncio.wait_for(
                        task, timeout, loop=connection._loop)
            except (asyncio.futures.TimeoutError, NotConnectedError) as e:
                # При возникновении ошибки закрываем соединение на
                # транспортном уровне.
                try:
                    if protocol.transport is not None:
                        asyncio.get_event_loop().call_soon(
                            protocol.transport.close)
                except Exception:
                    logging.exception(
                        "Error while handling redis operation timeout")
                raise NotConnectedError()
            return result

        return wrapper

    for method in _all_commands:
        setattr(cls, method,
                _timeout_aware_connection(getattr(RedisProtocol, method)))
    return cls


@timeout_aware_pool
class PoolWrapper(Pool):

    _pool_wrapper_fields = (
        '_reconnect',
        '_cleanup_connections',
        '_timeout',
        '_connect_timeout',
        '_loop'
    )

    @classmethod
    @asyncio.coroutine
    def create(cls, host='localhost', port=6379, password=None, db=0,
               encoder=None, poolsize=1, auto_reconnect=True, loop=None,
               timeout=1, connection_timeout=1):
        pool = cls()
        pool._host = host
        pool._port = port
        pool._poolsize = poolsize
        pool._connect_timeout = connection_timeout
        pool._timeout = timeout
        pool._loop = loop
        pool._auto_reconnect = auto_reconnect
        pool._pending_connections = []

        # Фабрика для создания новых соединений
        @asyncio.coroutine
        def connection_factory():
            task = Connection.create(
                host=host,
                port=port,
                password=password,
                db=db,
                encoder=encoder,
                auto_reconnect=auto_reconnect,
                loop=loop,
                # timeout=timeout,
                # connect_timeout=connection_timeout,
                # connection_lost_callback=pool._cleanup_connections)
            )
            task = asyncio.Task(task)
            pool._pending_connections.append(task)
            task.add_done_callback(pool._register_connection)
            return (yield from asyncio.wait_for(task, None))

        pool._connection_factory = connection_factory

        # Create connections
        pool._connections = []

        yield from pool._reconnect()

        return pool

    def _register_connection(self, task):
        self._pending_connections.remove(task)
        self._connections.append(task.result())

    @asyncio.coroutine
    def _reconnect(self):
        tasks = []
        for i in range(len(self._connections) + len(self._pending_connections), self._poolsize):
            task = asyncio.Task(self._connection_factory())
            tasks.append(task)
        if tasks:
            yield from asyncio.wait(tasks, timeout=self._connect_timeout)
            # if pending:
            #     for task in pending:
            #         task.set_exception(NotConnectedError())

    def _cleanup_connections(self):
        # def is_connected(c):
        #     return c.protocol.is_connected
        # # is_connected = lambda c: c.protocol.is_connected
        # self._connections = list(filter(is_connected, self._connections))
        # if self._auto_reconnect:
        #     asyncio.Task(self._reconnect())
        pass

    def __getattr__(self, item):
        if item in self.__class__._pool_wrapper_fields:
            return object.__getattribute__(self, item)
        try:
            return super().__getattr__(item)
        except:
            pass


@timeout_aware_conn
class ConnectionWrapper(Connection):
    """ Обертка поверх обычного Connection Pool клиента редиса для более
    удобной смены реализации клиента (таймауты и т.п.)
    """
    protocol = RedisProtocol

    def __init__(self, *args, **kwargs):
        try:
            kwargs.pop('timeout')
        except KeyError:
            pass
        super().__init__(*args, **kwargs)

    @classmethod
    @asyncio.coroutine
    def create(cls, host='localhost', port=6379, password=None, db=0,
               encoder=None, auto_reconnect=True, loop=None,
               connection_lost_callback=None, timeout=None,
               connect_timeout=1):
        connection = cls()

        connection.host = host
        connection.port = port
        connection._loop = loop
        connection._retry_interval = .5

        connection._connect_timeout = timeout
        connection._timeout = timeout

        # Create protocol instance
        def connection_lost():
            if connection_lost_callback:
                connection_lost_callback()
            if auto_reconnect:
                asyncio.Task(connection._reconnect())

        # Create protocol instance
        connection.protocol = RedisProtocol(
            password=password,
            db=db,
            encoder=encoder,
            connection_lost_callback=connection_lost)

        # Connect
        yield from connection._reconnect()

        return connection

    @asyncio.coroutine
    def _reconnect(self):
        task = super()._reconnect()
        #try:
        timeout = self._connect_timeout or 1
        done, pending = yield from asyncio.wait([task], timeout=timeout, loop=self._loop)
        if pending:
            task = pending.pop()
            task.set_exception(NotConnectedError())
            if self.transport is not None:
                self.transport.close()
            raise NotConnectedError()
        # except (asyncio.TimeoutError, NotConnectedError):
        #     try:
        #         if self.transport is not None:
        #             self.transport.close()
        #     except Exception:
        #         logging.exception(
        #             "Error while handling redis reconnect timeout")
        #     raise NotConnectedError()

    def __getattr__(self, item):
        if item not in ('_timeout', '_connect_timeout', '_loop', 'transport'):
            return super().__getattr__(item)
        return object.__getattribute__(self, item)


