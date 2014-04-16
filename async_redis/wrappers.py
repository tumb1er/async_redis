# coding: utf-8

import functools
import logging
import socket

import asyncio
from asyncio.log import logger
from asyncio_redis.exceptions import NotConnectedError
from asyncio_redis.protocol import _all_commands, RedisProtocol
from asyncio_redis import Connection


def timeout_aware_conn(cls):
    """ Декоратор, добавляющий таймауты для операций Redis
        для реализации Connection."""

    def _timeout_aware_connection(cmd):

        @asyncio.coroutine
        @functools.wraps(cmd)
        def wrapper(connection, *args, **kwargs):
            # Выполняем команду протокола Redis
            task = cmd(connection.protocol, *args, **kwargs)
            if not connection.protocol._is_connected:
                reconnect = asyncio.Task(connection._reconnect())
            elif connection._reconnect_task:
                reconnect = connection._reconnect_task
            else:
                reconnect = None

            if reconnect:
                try:
                    yield from asyncio.wait_for(reconnect, connection._connect_timeout)
                except asyncio.TimeoutError:
                    raise NotConnectedError('reconnect timeout')

            result = yield from task

            return result

        return wrapper

    for method in _all_commands:
        setattr(cls, method,
                _timeout_aware_connection(getattr(RedisProtocol, method)))
    return cls


class MoreRedisProtocol(RedisProtocol):
    def __init__(self, connection_made_callback=None, **kwargs):
        super().__init__(**kwargs)
        self._connection_made_callback = connection_made_callback

    def connection_made(self, transport):
        super().connection_made(transport)
        if self._connection_made_callback:
            self._connection_made_callback()

    def _push_answer(self, answer):
        """
        Answer future at the queue.
        """
        f = self._queue.popleft()

        if isinstance(answer, Exception):
            f.set_exception(answer)
        elif f.cancelled(): # Aga!
            pass
        else:
            f.set_result(answer)


@timeout_aware_conn
class ConnectionWrapper(Connection):
    """ Обертка поверх обычного Connection Pool клиента редиса для более
    удобной смены реализации клиента (таймауты и т.п.)
    """
    protocol = RedisProtocol

    def __init__(self, host='localhost', port=6379, **kwargs):
        self.host = host
        self.port = port
        self._retry_interval = .5
        self._reconnect_task = None
        for k,v in kwargs.items():
            setattr(self, '_' + k, v)

    @classmethod
    @asyncio.coroutine
    def create(cls, host='localhost', port=6379, password=None, db=0,
               encoder=None, auto_reconnect=True, loop=None,
               connection_lost_callback=None, connection_made_callback=None,
               timeout=None, connect_timeout=1, **kwargs):
        connection = cls(host=host,
                         port=port,
                         loop=loop,
                         connect_timeout=connect_timeout,
                         timeout=timeout,
                         auto_reconnect=auto_reconnect,
                         connection_lost_callback=connection_lost_callback,
                         connection_made_callback=connection_made_callback)

        # Create protocol instance
        connection.protocol = MoreRedisProtocol(
            password=password,
            db=db,
            encoder=encoder,
            connection_lost_callback=connection._connection_lost,
            connection_made_callback=connection._connection_made)

        # Connect
        try:
            yield from asyncio.wait_for(connection._reconnect(),
                                        timeout=connect_timeout)
        except asyncio.futures.TimeoutError:
            raise NotConnectedError('connect timeout')

        return connection

    @asyncio.coroutine
    def _create_connection(self, protocol_factory, host, port):
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
                self._reconnect_task = asyncio.Task(self._create_connection(lambda: self.protocol,
                                                    self.host, self.port))
                yield from asyncio.wait_for(self._reconnect_task, None)
                self._reset_retry_interval()
                self._reconnect_task = None
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

    def _connection_lost(self):
        print ("Connection lost!")
        if self._connection_lost_callback:
                self._connection_lost_callback()
        if self._auto_reconnect:
            asyncio.Task(self._reconnect())

    def _connection_made(self):
        print ("Connection made!")
        if self._connection_made_callback:
                self._connection_made_callback()


