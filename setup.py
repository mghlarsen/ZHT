#!/usr/bin/env python
from setuptools import setup
from zht.version import packageVersion

setup(
    name='ZHT',
    version=packageVersion,
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

