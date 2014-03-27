# coding: utf-8

# $Id: $
from functools import wraps
import asyncio


def async(test_method):
    """ Декоратор, превращающий тест в асинхронный.

    @param test_method: asyncio.coroutine
    """

    @wraps(test_method)
    def inner(self, *args, **kwargs):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        try:
            self.loop.run_until_complete(test_method(self, *args, **kwargs))
        finally:
            asyncio.set_event_loop(None)
            self.loop = None
    return inner
