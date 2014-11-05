
__author__    = "Radical.Utils Development Team (Andre Merzky, Ole Weidner)"
__copyright__ = "Copyright 2013, RADICAL@Rutgers"
__license__   = "MIT"


''' Provides the default log output formatter.
'''

from logging import Formatter

# DefaultFormatter = Formatter(fmt=' %(funcName)-15s:%(module)-15s %(lineno)4d %(asctime)s %(name)-23s: [%(levelname)-8s] %(message)s', 
DefaultFormatter = Formatter(fmt='%(asctime)s %(process)-6d %(threadName)-12s %(name)-22s: [%(levelname)-8s] %(message)s', 
                             datefmt='%Y:%m:%d %H:%M:%S')



