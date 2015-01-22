
__author__    = "Radical.Utils Development Team (Andre Merzky)"
__copyright__ = "Copyright 2013, RADICAL@Rutgers"
__license__   = "MIT"


import sys
import threading
import traceback

import misc  as rumisc


_out_lock = threading.RLock ()


# ------------------------------------------------------------------------------
#
NEW     = 'New'
RUNNING = 'Running'
DONE    = 'Done'
FAILED  = 'Failed'


# ------------------------------------------------------------------------------
#
def lout (txt, stream=sys.stdout) :

    with _out_lock :
        stream.write (txt)
        stream.flush ()


# ------------------------------------------------------------------------------
#
def Event (*args, **kwargs) :
    return threading.Event (*args, **kwargs)


# ------------------------------------------------------------------------------
#
class RLock (object) :
    # 
    # see http://stackoverflow.com/questions/6780613/
    #     is-it-possible-to-subclass-lock-objects-in-python-if-not-other-ways-to-debug

    # --------------------------------------------------------------------------
    #
    def __init__ (self, obj=None) :

        self._lock = threading.RLock ()

      # with self._lock :
      #     self._obj = obj
      #     self._cnt = 0


    # --------------------------------------------------------------------------
    #
    def acquire (self) :

      # ind = (self._cnt)*' '+'>'+(30-self._cnt)*' '
      # lout ("%s -- %-10s %50s acquire  - %s\n" % (ind, threading.current_thread().name, self, self._lock))

        self._lock.acquire ()

      # self._cnt += 1
      # ind = (self._cnt)*' '+'|'+(30-self._cnt)*' '
      # lout ("%s    %-10s %50s acquired - %s\n" % (ind, threading.current_thread().name, self, self._lock))


    # --------------------------------------------------------------------------
    #
    def release (self) :

      # ind = (self._cnt)*' '+'-'+(30-self._cnt)*' '
      # lout ("%s    %-10s %50s release  - %s\n" % (ind, threading.current_thread().name, self, self._lock))

        self._lock.release ()

      # self._cnt -= 1
      # ind = (self._cnt)*' '+'<'+(30-self._cnt)*' '
      # lout ("%s -- %-10s %50s released - %s\n" % (ind, threading.current_thread().name, self, self._lock))


    # --------------------------------------------------------------------------
    #
    def __enter__ (self)                         : self.acquire () 
    def __exit__  (self, type, value, traceback) : self.release ()



# ------------------------------------------------------------------------------
#
class Thread (threading.Thread) :

    # --------------------------------------------------------------------------
    #
    def __init__ (self, call, *args, **kwargs) :

        if not callable (call) :
            raise ValueError ("Thread requires a callable to function, not %s" \
                           % (str(call)))

        threading.Thread.__init__ (self)

        self._call      = call
        self._args      = args
        self._kwargs    = kwargs
        self._state     = NEW
        self._result    = None
        self._exception = None
        self.daemon     = True


    # --------------------------------------------------------------------------
    #
    @classmethod
    def Run (self, call, *args, **kwargs) :

        t = self (call, *args, **kwargs)
        t.start ()
        return t


    # --------------------------------------------------------------------------
    #
    @property 
    def tid (self) :
        return self.tid


    # --------------------------------------------------------------------------
    #
    def run (self) :

        try :
            self._state     = RUNNING
            self._result    = self._call (*self._args, **self._kwargs)
            self._state     = DONE

        except Exception as e :
            tb = traceback.format_exc()
            print "thread got exception: %s:%s%s" % (type(e).__name__, str(e), tb)

            self._exception = e
            self._state     = FAILED


    # --------------------------------------------------------------------------
    #
    def wait (self) :

        if  self.isAlive () :
            self.join ()


    # --------------------------------------------------------------------------
    #
    def cancel (self) :
        # FIXME: this is not really implementable generically, so we ignore 
        # cancel requests for now.
        pass


    # --------------------------------------------------------------------------
    #
    def get_state (self) :
        return self._state 

    state = property (get_state)


    # --------------------------------------------------------------------------
    #
    def get_result (self) :

        if  self._state == DONE :
            return self._result

        return None

    result = property (get_result)


    # --------------------------------------------------------------------------
    #
    def get_exception (self) :

        if  self._state == FAILED :
            return self._exception 

        return None

    exception = property (get_exception)


# ------------------------------------------------------------------------------
#


