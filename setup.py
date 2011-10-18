from setuptools import setup

setup(
    name='ZHT',
    version='0.0.1dev',
    author='Michael Larsen',
    author_email='mike.gh.larsen@gmail.com',
    packages=['zht'],
    url='http://github.com/mghlarsen/zht',
    description='ZeroMQ-based distributed hash table',
    long_description=open('README.rst').read(),
    install_requires=[
        'gevent-zeromq>=0.2.0',
        'pyzmq>=2.1',
    ]
)

