# coding: utf-8

# $Id: $
import asyncio
import asyncio_redis


connection_errors = (
        asyncio.TimeoutError,
        ConnectionRefusedError,
        asyncio_redis.NotConnectedError
    )
