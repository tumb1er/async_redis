from distutils.core import setup

setup(
    name='async_redis',
    version='0.10.0',
    packages=['async_redis'],
    url='',
    license='Beer Licence',
    author='tumbler',
    author_email='zimbler@gmail.com',
    description='High-availability wrapper of asyncio_redis',
    install_requires=['asyncio>=0.4', 'asyncio_redis>=0.13,<0.14'],
    test_requires=['hiredis']
)
