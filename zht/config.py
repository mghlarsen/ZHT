#
# Copyright 2011 Michael Larsen <mike.gh.larsen@gmail.com>
#
"""
Handle configuring ZHT from the command line and from configuration files
"""
from argparse import ArgumentParser
import ConfigParser

_argParser = ArgumentParser("DHT Node")
_argParser.add_argument('--bindAddrREP', '-r')
_argParser.add_argument('--bindAddrPUB', '-p')
_argParser.add_argument('--connectAddr', '-c', required=False)
_argParser.add_argument('--identity', '-i', required=False)
_argParser.add_argument('--config', '-C', default='.zhtrc', required=False)


class ZHTConfig(ConfigParser.SafeConfigParser):
    """
    Get the configuration provided from config files and command-line arguments.

    Note that currently the configuration lookup uses the "zht" section of the config file and must have the same key
    as the command-line options.
    """
    def __init__(self, defaults=None, *args):
        self.__defaults = defaults
        self.__args = _argParser.parse_args()
        
        ConfigParser.SafeConfigParser.__init__(self, self.__defaults, *args)
        self.read(self.__args.config)

    def __getattr__(self, attr):
        try:
            res = getattr(self.__args, attr)
            if not res is None:
                return res
        except AttributeError:
            pass
        return self.__configLookup(attr) 

    def __getitem__(self, idx):
        return self.__getattr__(idx)

    def __configLookup(self, attr):
        try:
            return self.get('zht', attr)
        except ConfigParser.NoOptionError:
            return None

