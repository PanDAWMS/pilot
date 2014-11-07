
__author__    = "Radical.Utils Development Team (Andre Merzky, Ole Weidner)"
__copyright__ = "Copyright 2013, RADICAL@Rutgers"
__license__   = "MIT"


# import utility classes
from object_cache   import ObjectCache
from plugin_manager import PluginManager
from singleton      import Singleton
from threads        import Thread, RLock, NEW, RUNNING, DONE, FAILED
from url            import Url
from dict_mixin     import DictMixin, dict_merge, dict_stringexpand
from lockable       import Lockable
from registry       import Registry, READONLY, READWRITE
from regex          import ReString, ReSult
from reporter       import Reporter
from benchmark      import Benchmark
from lease_manager  import LeaseManager

# import utility methods
from ids            import generate_id, ID_SIMPLE, ID_UNIQUE
from read_json      import read_json
from read_json      import read_json_str
from read_json      import parse_json
from read_json      import parse_json_str
from tracer         import trace, untrace
from which          import which
from misc           import split_dburl, mongodb_connect
from misc           import parse_file_staging_directives 
from misc           import time_diff
from misc           import DebugHelper
from get_version    import get_version

# import decorators
from timing         import timed_method

# import sub-modules
# from config         import Configuration, Configurable, ConfigOption, getConfig


# ------------------------------------------------------------------------------


import os

_mod_root = os.path.dirname (__file__)

version        =   "0.7.8" #open (_mod_root + "/VERSION",     "r").readline ().strip ()
version_detail =   "standalone" #open (_mod_root + "/VERSION.git", "r").readline ().strip ()


# ------------------------------------------------------------------------------

