
__author__    = "Andre Merzky"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"


<<<<<<< HEAD
import saga.adaptors.cpi.base       as cpi_base
import saga.adaptors.cpi.decorators as cpi_dec

=======
import saga.adaptors.cpi.decorators as cpi_dec
import saga.adaptors.cpi.base       as cpi_base
>>>>>>> origin/titan

SYNC  = cpi_dec.CPI_SYNC_CALL
ASYNC = cpi_dec.CPI_ASYNC_CALL

class Context (cpi_base.CPIBase) :
    
    @SYNC
    def init_instance         (self, type)    : pass

    @SYNC
    def _initialize           (self, session) : pass

    @SYNC
    def _get_default_contexts (self, session) : pass


<<<<<<< HEAD
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4
=======

>>>>>>> origin/titan

