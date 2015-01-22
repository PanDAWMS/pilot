
__author__    = "Andre Merzky"
<<<<<<< HEAD
__copyright__ = "Copyright 2013, The SAGA Project"
=======
__copyright__ = "Copyright 2012-2013, The SAGA Project"
>>>>>>> origin/titan
__license__   = "MIT"


""" Monitorable interface """

<<<<<<< HEAD
import saga.attributes       as sa
import saga.base             as sb
import saga.exceptions       as se
import saga.utils.signatures as sus
=======
import radical.utils.signatures as rus

import saga.attributes       as sa
import saga.base             as sb
import saga.exceptions       as se
>>>>>>> origin/titan


# ------------------------------------------------------------------------------
#
class Monitorable (sa.Attributes) :

    # --------------------------------------------------------------------------
    #
    def __init__ (self) :

        self._attr = super (Monitorable, self)
        self._attr.__init__ ()


    # --------------------------------------------------------------------------
    # 
    # since we have no means to call the saga.Base constructor explicitly (we
    # don't inherit from it), we have to rely that the classes which implement
    # the Monitorable interface are correctly calling the Base constructure --
    # otherwise we won't have an self._adaptor to talk to...
    #
    # This helper method checks the existence of self._adaptor, and should be
    # used before each call forwarding.
    #
    def _check (self) :
        if  not hasattr (self, '_adaptor') :
            raise se.IncorrectState ("object is not fully initialized")


    # --------------------------------------------------------------------------
    #
<<<<<<< HEAD
    @sus.takes   ('Monitorable')
    @sus.returns (sus.list_of (basestring))
=======
    @rus.takes   ('Monitorable')
    @rus.returns (rus.list_of (basestring))
>>>>>>> origin/titan
    def list_metrics (self) :

        self._check ()
        return self._adaptor.list_metrics ()


    # --------------------------------------------------------------------------
    #
    # Metrics are not implemented in SAGA-Python
    #
<<<<<<< HEAD
  # @sus.takes   ('Monitorable', basestring)
  # @sus.returns ('Metric')
=======
  # @rus.takes   ('Monitorable', basestring)
  # @rus.returns ('Metric')
>>>>>>> origin/titan
  # def get_metric (name) :
  #
  #     self._check ()
  #     return self._adaptor.get_metric (name)


    # --------------------------------------------------------------------------
    #
<<<<<<< HEAD
    @sus.takes   ('Monitorable',
                  basestring,
                  sus.one_of ('saga.Callback', callable))
    @sus.returns (int)
=======
    @rus.takes   ('Monitorable',
                  basestring,
                  rus.one_of ('saga.Callback', callable))
    @rus.returns (int)
>>>>>>> origin/titan
    def add_callback (self, name, cb) :

        self._check ()
        return self._adaptor.add_callback (name, cb)


    # --------------------------------------------------------------------------
    #
<<<<<<< HEAD
    @sus.takes   ('Monitorable',
                  int)
    @sus.returns (sus.nothing)
=======
    @rus.takes   ('Monitorable',
                  int)
    @rus.returns (rus.nothing)
>>>>>>> origin/titan
    def remove_callback (self, cookie) :

        self._check ()
        return self._adaptor.remove_callback (cookie)



<<<<<<< HEAD
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4
=======

>>>>>>> origin/titan

