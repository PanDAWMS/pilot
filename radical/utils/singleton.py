
__author__    = "Radical.Utils Development Team (Andre Merzky)"
__copyright__ = "Copyright 2013, RADICAL@Rutgers"
__license__   = "MIT"


import threading


_singleton_lock = threading.RLock ()


""" Provides a Singleton metaclass.  """

# ------------------------------------------------------------------------------
#
class Singleton (type) :
    """ 
    A metaclass to 'tag' other classes as singleton::

        class MyClass(BaseClass):
            __metaclass__ = Singleton

    """
    _instances = {}

    def __call__(cls, *args, **kwargs):

        with _singleton_lock :

            if  cls not in cls._instances:
                cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)

            return cls._instances[cls]


# ------------------------------------------------------------------------------
#


