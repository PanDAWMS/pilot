
__author__    = "Andre Merzky"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"


<<<<<<< HEAD
import saga.adaptors.cpi.decorators as cpi_dec
import saga.exceptions              as se

=======
import saga.exceptions              as se
import saga.adaptors.cpi.decorators as cpi_dec
>>>>>>> origin/titan

SYNC  = cpi_dec.CPI_SYNC_CALL
ASYNC = cpi_dec.CPI_ASYNC_CALL


class Attributes (object) :
    
    @SYNC
    def attribute_getter    (self, key)         : pass

    @SYNC
    def attribute_setter    (self, key, val)    : pass

    @SYNC
    def attribute_lister    (self)              : pass

    @SYNC
    def attribute_caller    (self, key, id, cb) : pass

    @SYNC
    def add_callback        (self, key, cb)     :
        raise se.NotImplemented ("Callbacks are not supported for this backend")



<<<<<<< HEAD
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4
=======

>>>>>>> origin/titan

