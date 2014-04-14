# coding: utf-8

import functools
import logging
import socket

import asyncio
from asyncio.log import logger
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
            # выбираем свободное соединение
            connection = pool._get_free_connection()
            if not connection:
                raise NotConnectedError()
            # пробуем переустановить соединение
            reconnect = object.__getattribute__(connection, '_reconnect')
            yield from reconnect()
            # вызываем команду редиса
            task = cmd(connection.protocol, *args, **kwargs)
            result = yield from task
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
            # При необходимости оборачиваем в wait_for
            timeout = connection._timeout
            closer = None
            if timeout:
                def close_conn():
                    protocol.transport.close()
                closer = connection._loop.call_later(timeout, close_conn)
            result = yield from task
            if closer:
                closer.cancel()
            return result

        return wrapper

    for method in _all_commands:
        setattr(cls, method,
                _timeout_aware_connection(getattr(RedisProtocol, method)))
    return cls


#@timeout_aware_pool
class PoolWrapper(Pool):
    @classmethod
    @asyncio.coroutine
    def create(cls, host='localhost', port=6379, password=None, db=0,
               encoder=None, poolsize=1, auto_reconnect=True, loop=None,
               timeout=None, connect_timeout=None):
        self = cls()
        self._host = host
        self._port = port
        self._poolsize = poolsize

        # Create connections
        self._connections = []

        for i in range(poolsize):
            connection = yield from ConnectionWrapper.create(
                host=host, port=port, password=password, db=db, encoder=encoder,
                auto_reconnect=auto_reconnect, loop=loop,
                timeout=timeout, connect_timeout=connect_timeout)
            self._connections.append(connection)

        return self


@timeout_aware_conn
class ConnectionWrapper(Connection):
    """ Обертка поверх обычного Connection Pool клиента редиса для более
    удобной смены реализации клиента (таймауты и т.п.)
    """
    protocol = RedisProtocol

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

        connection._connect_timeout = connect_timeout
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
        try:
            yield from asyncio.wait_for(connection._reconnect(),
                                        timeout=connect_timeout)
        except asyncio.futures.TimeoutError:
            raise NotConnectedError()

        return connection

    @asyncio.coroutine
    def create_connection(self, protocol_factory, host, port):
        f1 = self._loop.getaddrinfo(host, port, family=0,
                                    type=socket.SOCK_STREAM, proto=0, flags=0)
        yield from asyncio.wait([f1], loop=self._loop)
        infos = f1.result()
        if not infos:
            raise OSError('getaddrinfo() returned empty list')
        exceptions = []
        for family, type, proto, cname, address in infos:
            try:
                sock = socket.socket(family=family, type=type, proto=proto)
                sock.setblocking(False)
            except OSError as exc:
                if sock is not None:
                    sock.close()
                exceptions.append(exc)
            else:
                break
        else:
            if len(exceptions) == 1:
                raise exceptions[0]
            else:
                # If they all have the same str(), raise one.
                model = str(exceptions[0])
                if all(str(exc) == model for exc in exceptions):
                    raise exceptions[0]
                # Raise a combined exception so the user can see all
                # the various error messages.
                raise OSError('Multiple exceptions: {}'.format(
                    ', '.join(str(exc) for exc in exceptions)))

        try:
            task = self._loop.sock_connect(sock, address)
            yield from asyncio.wait_for(task, timeout=self._connect_timeout,
                                        loop=self._loop)

            task = self._loop.create_connection(protocol_factory,
                                                sock=sock)
            yield from asyncio.wait_for(task, timeout=self._connect_timeout,
                                        loop=self._loop)
        except (asyncio.futures.TimeoutError, IOError,
                asyncio.futures.CancelledError) as e:

            try:
                self._loop.remove_writer(sock.fileno())
                pass
            except Exception as e:
                pass
            sock.close()
            raise
        except Exception as e:
            pass
            raise

    def close(self):
        if self.transport:
            self.transport.close()
        self.protocol._connection_lost_callback = None

    @asyncio.coroutine
    def _reconnect(self):
        self._loop = self._loop or asyncio.get_event_loop()
        while True:
            try:
                logger.log(logging.INFO, 'Connecting to redis')
                yield from self.create_connection(lambda: self.protocol,
                                                  self.host, self.port)
                self._reset_retry_interval()
                return
            except OSError as e:
                # Sleep and try again
                self._increase_retry_interval()
                interval = self._get_retry_interval()
                logger.log(logging.INFO,
                           'Connecting to redis failed. Retrying in %i seconds' % interval)
                yield from asyncio.sleep(interval)
            except Exception as e:
                raise

                #
                #
                # task = super()._reconnect()
                # #try:
                # timeout = self._connect_timeout or 1
                # done, pending = yield from asyncio.wait([task], timeout=timeout, loop=self._loop)
                # if pending:
                #     task = pending.pop()
                #     task.set_exception(NotConnectedError())
                #     if self.transport is not None:
                #         self.transport.close()
                #     raise NotConnectedError()




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


