ZHT - ZeroMQ Hash Table
=======================

ZHT_ is a distributed hash table (and protocol) implemented on top of the ZeroMQ messaging library.

.. _ZHT: https://github.com/mghlarsen/ZHT

Installation
------------

With `pip` (you most likely want to do this in a virtualenv)::

   pip install https://github.com/mghlarsen/ZHT/tarball/v0.0.2

(You can replace v0.0.2 with any other tag)
OR::

   pip install -e git+git://github.com/mghlarsen/ZHT.git@v0.0.2#egg=ZHT

(You can change the part after the @ to be any tag/branch/revision).

WARNING: ZHT is in a really early state, and I can't guarantee that it won't blow up your house/computer/municipality,
much less that it will work properly. You may have to install a more-recent-than-latest-release version of greenlet
(there's a version that works for me in `requirements.txt`, although YMMV).

Or you can just check out ZHT and run from the project directory, since it's not really built to be used by anything
yet. All the packages you'll want can be installed by running::

   pip install -r requirements.txt

As long as you have a new-enough libzeromq (for pyzmq) and libevent (for gevent), this should work fine.

Usage
-----

You can start a ZHT node by running::

   python -m zht.shell

This will try to take configuration options from `.zhtrc` in the current directory. Use `-h` to find out what kind of
options are available, and note that `.zhtrc` should be ini-style and responds to settings in a [zht] section. If
you want to get rid of the logging warning/want to see log output, put settings in a `.zhtloggingrc` file. This
should be in a format compatible with logging.config.fileConfig(). All ZHT logging is in the `zht` domain, with
subdomains defined for each module in ZHT.

You can also run the test suite by running::

   python setup.py nosetests

Keep in mind that you'll need `nose` and probably `coverage` (you could skip that by messing with `setup.cfg`, but
it's in requirements.txt) for this.

You can generate the HTML docs by running::

   python setup.py build_sphinx

Note that you'll need `Sphinx` for this. `requirements.txt` includes it.

