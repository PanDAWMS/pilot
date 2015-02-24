
__author__    = "Radical.Utils Development Team (Andre Merzky, Ole Weidner)"
__copyright__ = "Copyright 2013, RADICAL@Rutgers"
__license__   = "MIT"


from   logging import DEBUG, INFO, WARNING, WARN, ERROR, CRITICAL
from   logger  import getLogger

import radical.utils as ru

# ------------------------------------------------------------------------------
def log_version (module, name, version, version_detail) :

    _log = getLogger (module)
    _log.info ('%-15s version: %s (%s)', name, version, version_detail)

log_version ('radical', 'radical.utils', ru.version, ru.version_detail)


# ------------------------------------------------------------------------------

