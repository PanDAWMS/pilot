
__author__    = "Radical.Utils Development Team (Andre Merzky, Ole Weidner)"
__copyright__ = "Copyright 2013, RADICAL@Rutgers"
__license__   = "MIT"


''' Provides log handler management. '''

import re
import logging

import radical.utils         as ru
import radical.utils.config  as ruc

from   radical.utils.logger.colorstreamhandler import *
from   radical.utils.logger.filehandler        import FileHandler
from   radical.utils.logger.defaultformatter   import DefaultFormatter

_log_configs = dict ()

def _get_log_config (name) :

    if  not name in _log_configs :
    
        _log_configs[name] = _LogConfig (name)

    return _log_configs[name]
    

# ------------------------------------------------------------------------------
#
# These are all supported options for logging
#
# ------------------------------------------------------------------------------
#
class _LogConfig (ruc.Configurable) :
    """
    :todo: documentation.  Also, documentation of options are insufficient
    (like, what are valid options for 'target'?)

    This class is not to be directly used by applications.
    """

    # --------------------------------------------------------------------------
    #
    class _MultiNameFilter(logging.Filter):
        def __init__(self, pos_filters, neg_filters=[]):
            self._pos_filters = pos_filters
            self._neg_filters = neg_filters

        def filter(self, record):
            print_it = False

            if not len(self._pos_filters) :
                print_it = True
            else :
                for f in self._pos_filters:
                    if  f in record.name:
                        print_it = True

            for f in self._neg_filters:
                if  f in record.name:
                    print_it = False

            return print_it

    # --------------------------------------------------------------------------
    #
    def __init__ (self, name) :

        self._name    = name
        self._lc_name = name.lower ()
        self._uc_name = re.sub (r"[^A-Z0-9]+", "_", name.upper ())

        _all_logging_options = [
            { 
            'category'      : '%s.logging' % self._lc_name,
            'name'          : 'level', 
            'type'          : str, 
            'default'       : 'CRITICAL', 
            'valid_options' : ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
            'documentation' : 'The log level',
            'env_variable'  : '%s_VERBOSE' % self._uc_name
            },
            { 
            'category'      : '%s.logging' % self._lc_name,
            'name'          : 'filters', 
            'type'          : list, 
            'default'       : [], 
            'valid_options' : None,
            'documentation' : 'The log filters',
            'env_variable'  : '%s_LOG_FILTER' % self._uc_name
            },
            { 
            'category'      : '%s.logging' % self._lc_name,
            'name'          : 'targets', 
            'type'          : list, 
            'default'       : ['STDOUT'], 
            'valid_options' : None,
            'documentation' : 'The log targets',
            'env_variable'  : '%s_LOG_TARGETS' % self._uc_name
            },
            { 
            'category'      : '%s.logging' % self._lc_name,
            'name'          : 'ttycolor', 
            'type'          : bool, 
            'default'       : True, 
            'valid_options' : [True, False],
            'documentation' : 'Whether to use colors for console output or not.',
            'env_variable'  : '%s_LOG_TTYCOLOR' % self._uc_name
            },
        ]

        ruc.Configurable.__init__       (self, self._lc_name)
        ruc.Configurable.config_options (self, '%s.logging' % self._lc_name,
                                         _all_logging_options) 
        cfg = self.get_config('%s.logging' % self._lc_name,)

        self._loglevel = str(cfg['level'].get_value())
        self._filters  = cfg['filters'].get_value()
        self._targets  = cfg['targets'].get_value()
        self._handlers = list()

        if self._loglevel is not None:
            if self._loglevel.isdigit():
                if   int(self._loglevel)    >= 4:           self._loglevel = logging.DEBUG
                elif int(self._loglevel)    == 3:           self._loglevel = logging.INFO
                elif int(self._loglevel)    == 2:           self._loglevel = logging.WARNING
                elif int(self._loglevel)    == 1:           self._loglevel = logging.ERROR
                elif int(self._loglevel)    == 0:           self._loglevel = logging.CRITICAL
                else: raise ValueError ('%s is not a valid value for %s_VERBOSE.' \
                                     % (self._loglevel, self._uc_name))
            else:
                if   self._loglevel.lower() == 'debug':     self._loglevel = logging.DEBUG
                elif self._loglevel.lower() == 'info':      self._loglevel = logging.INFO
                elif self._loglevel.lower() == 'warning':   self._loglevel = logging.WARNING
                elif self._loglevel.lower() == 'error':     self._loglevel = logging.ERROR
                elif self._loglevel.lower() == 'critical':  self._loglevel = logging.CRITICAL
                else: raise ValueError ('%s is not a valid value for %s_VERBOSE.' \
                                     % (self._loglevel, self._uc_name))

        # create the handlers (target + formatter + filter)
        for target in self._targets:

            if target.lower() == 'stdout':
                # create a console stream logger
                # Only enable colour if support was loaded properly
                if has_color_stream_handler is True:
                    handler = ColorStreamHandler()
                else: 
                    handler = logging.StreamHandler()
            else:
                # got to be a file logger
                handler = FileHandler(target)

            handler.setFormatter(DefaultFormatter)

            if self._filters != []:
                pos_filters = []
                neg_filters = []

                for f in self._filters :
                    if  f and f[0] == '!' :
                        neg_filters.append (f[1:])
                    else :
                        pos_filters.append (f)

                handler.addFilter(self._MultiNameFilter (pos_filters, neg_filters))

            self._handlers.append(handler)


    # --------------------------------------------------------------------------
    #
    @property
    def loglevel(self):
        return self._loglevel

    # --------------------------------------------------------------------------
    #
    @property
    def handlers(self):
        return self._handlers


# ------------------------------------------------------------------------------
#
# a singleton class which keeps all loggers
#
_logger_registry = ru.ObjectCache ()


# ------------------------------------------------------------------------------
#
# FIXME: strange pylint error
#
def getLogger (name, tag=None):
    ''' 
    Get a logger.  For any new name/tag pair, a new logger instance will be
    created; subsequent calls to this method with the same argument set will
    return the same instance.

    Configuration for the logger is *only* derived from the name part.
    '''

    fullname = name
    if  tag :
        fullname += ".%s" % tag


    # get or create a python logger
    def creator () :
        return logging.getLogger (fullname)

    ret   = _logger_registry.get_obj (fullname, creator)
    lconf = _get_log_config (name)

    # was this logger initialized before?
    if ret.handlers == []:

        # initialize that logger
        for handler in lconf.handlers :
            ret.addHandler (handler)
       
        ret.setLevel (lconf.loglevel)
        ret.propagate = 0 # Don't bubble up to the root logger

    # setup done - we can return the logger
    return ret


# ------------------------------------------------------------------------------

