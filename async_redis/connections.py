# coding: utf-8

# $Id: $
import functools

import asyncio
from asyncio_redis import Connection
from async_redis import ConnectionWrapper


class Connections:
    """ Кэш коннектов к редису."""
    instance = None


    def __init__(self, **redis_creds):
        """
        @param redis_creds:
            Словарь настроек коннектов к редису в виде:
            alias: {
                host:
                port:
                db:
                timeout:
                connect_timeout:
                poolsize:
            }

            или просто настройки единственного коннекта:

            {
                host:
                port:
                db:
                timeout:
                connect_timeout:
                poolsize:
            }
        """
        if 'host' in redis_creds:
            redis_creds = {'default': redis_creds}

        if self.instance is not None:
            self.__conn_cache = self.instance.__conn_cache
            self.__settings = self.instance.__settings
        else:
            self.__settings = redis_creds
            self.__conn_cache = {}
            self.__class__.instance = self

    @asyncio.coroutine
    def get_connection(self, alias='default', wrapped=True):
        conn_kwargs = self.__settings[alias]
        # XXX
        if wrapped:
            klass = ConnectionWrapper
        else:
            klass = Connection
            wrapped_kwargs = 'timeout', 'connect_timeout', 'poolsize'
            conn_kwargs = {k: v for k, v in conn_kwargs.items()
                           if k not in wrapped_kwargs}

        if alias not in self.__conn_cache:
            # создаем новое соединение
            future = asyncio.Task(klass.create(**conn_kwargs))
            future.add_done_callback(
                functools.partial(self.register_connection, alias=alias))
            self.__conn_cache[alias] = future
        # есть коннект к кэше
        if not isinstance(self.__conn_cache[alias], asyncio.Task):
            return self.__conn_cache[alias]
        # в кэше лежит future, т.е. соединение в процессе открытия.
        conn = yield from asyncio.wait_for(self.__conn_cache[alias], None)
        return conn

    def register_connection(self, future, alias):
        conn = future.result()
        self.__conn_cache[alias] = conn

    def close_connection(self, alias='default'):
        connection = self.__conn_cache.pop(alias, None)
        if not connection:
            return
        try:
            connection.close()
        except Exception as e:
            pass
