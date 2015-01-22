
<<<<<<< HEAD
__author__    = "Andre Merzky, Ole Weidner"
=======
__author__    = "Andre Merzky"
>>>>>>> origin/titan
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"


""" Provides the CPI base class and a number of call decorators. """

import weakref

<<<<<<< HEAD
from   saga.exceptions import *
import saga.utils.config     as suc
import saga.utils.logger     as sul
=======
import radical.utils.config  as ruc
import radical.utils.logger  as rul

from   saga.exceptions import *
>>>>>>> origin/titan


# ------------------------------------------------------------------------------
#
# CPI base class
#
<<<<<<< HEAD
class CPIBase (suc.Configurable) :
=======
class CPIBase (ruc.Configurable) :
>>>>>>> origin/titan

    # --------------------------------------------------------------------------
    #
    def __init__ (self, api, adaptor) :

        self._session   = None
        self._adaptor   = adaptor
        self._cpi_cname = self.__class__.__name__
<<<<<<< HEAD
        self._logger    = sul.getLogger (self._cpi_cname)
=======
        self._logger    = rul.getLogger ('saga', self._cpi_cname)
>>>>>>> origin/titan

        # The API object must obviously keep an adaptor instance.  If we also
        # keep an API instance ref in this adaptor base, we create a ref cycle
        # which will annoy (i.e. disable) garbage collection.  We thus use weak
        # references to break that cycle.  The inheriting classes MUST use
        # get_api() to obtain the API reference.
<<<<<<< HEAD
        if api :
            self._api   = weakref.ref (api)
        else :
            self._api   = None
=======
      # if  api :
      #     self._api   = weakref.ref (api)
      # else :
      #     self._api   = None
        self._api   = weakref.ref (api)
>>>>>>> origin/titan

        # by default, we assume that no bulk optimizations are supported by the
        # adaptor class.  Any adaptor class supporting bulks ops must overwrite
        # the ``_container`` attribute (via
        # ``self._set_container(container=None)``, and have it point to the
        # class which implements the respective ``container_*`` methods.
        self._container = None


    def _set_container (self, container=None) :
        self._container = container


    def get_cpi_cname (self) :
        return self._cpi_cname


    def get_api (self) :
<<<<<<< HEAD
        if self._api :
            # get api from weakref.  We can be quite confident that the api
            # object has *not* been garbage collected, yet, as it obviously is
            # still binding this adaptor instance.
            return self._api ()
        else :
            # no need to de-weakref 'None'
            return self._api
=======

        # get api from weakref.  We can be quite confident that the api
        # object has *not* been garbage collected, yet, as it obviously is
        # still binding this adaptor instance.
        return self._api ()

    #   if self._api :
    #       # get api from weakref.  We can be quite confident that the api
    #       # object has *not* been garbage collected, yet, as it obviously is
    #       # still binding this adaptor instance.
    #       return self._api ()
    #   else :
    #       # no need to de-weakref 'None'
    #       return self._api
>>>>>>> origin/titan


    def get_adaptor_name (self) :
        return self._adaptor.get_name ()


    def _set_session (self, session) :
        self._session = session


    def get_session (self) :
        return self._session


<<<<<<< HEAD
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4
=======

>>>>>>> origin/titan

