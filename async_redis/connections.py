# coding: utf-8

# $Id: $

import asyncio
from async_redis import PoolWrapper, ConnectionWrapper


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
    def get_connection(self, alias='default'):
        conn_kwargs = self.__settings[alias]
        if 'poolsize' in conn_kwargs:
            klass = PoolWrapper
        else:
            klass = ConnectionWrapper
        if alias not in self.__conn_cache:
            self.__conn_cache[alias] = asyncio.Task(klass.create(**conn_kwargs))
        if not isinstance(self.__conn_cache[alias], asyncio.Task):
            return self.__conn_cache[alias]
        self.__conn_cache[alias] = yield from self.__conn_cache[alias]
        return self.__conn_cache[alias]

    def close_connection(self, alias='default'):
        connection = self.__conn_cache.pop(alias, None)
        if not connection:
            return
        try:
            connection.close()
        except Exception as e:
            pass
