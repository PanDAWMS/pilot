
<<<<<<< HEAD
__author__    = "Andre Merzky"
=======
__author__    = "Andre Merzky, Ole Weidner, Alexander Grill"
>>>>>>> origin/titan
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"


<<<<<<< HEAD
import saga.adaptors.base        as sab
from   saga.constants            import SYNC, ASYNC, TASK
from   saga.filesystem.constants import *
import saga.namespace.entry      as nsentry
import saga.session              as ss
import saga.task                 as st
import saga.url                  as surl
import saga.utils.signatures     as sus

=======
import radical.utils.signatures  as rus

import saga.adaptors.base        as sab
import saga.session              as ss
import saga.task                 as st
import saga.url                  as surl
import saga.namespace.entry      as nsentry

from   saga.filesystem.constants import *
from   saga.constants            import SYNC, ASYNC, TASK
>>>>>>> origin/titan

# ------------------------------------------------------------------------------
#
class File (nsentry.Entry) :
    """
    Represents a local or remote file.

    The saga.filesystem.File class represents, as the name indicates,
    a file on some (local or remote) filesystem.  That class offers
    a number of operations on that file, such as copy, move and remove::
    
        # get a file handle
        file = saga.filesystem.File("sftp://localhost/tmp/data/data.bin")
    
        # copy the file
        file.copy ("sftp://localhost/tmp/data/data.bak")

        # move the file
        file.move ("sftp://localhost/tmp/data/data.new")
    """

    # --------------------------------------------------------------------------
    #
<<<<<<< HEAD
    @sus.takes   ('File', 
                  sus.optional ((surl.Url, basestring)), 
                  sus.optional (int), 
                  sus.optional (ss.Session),
                  sus.optional (sab.Base), 
                  sus.optional (dict), 
                  sus.optional (sus.one_of (SYNC, ASYNC, TASK)))
    @sus.returns (sus.nothing)
=======
    @rus.takes   ('File', 
                  rus.optional ((surl.Url, basestring)), 
                  rus.optional (int, rus.nothing), 
                  rus.optional (ss.Session),
                  rus.optional (sab.Base), 
                  rus.optional (dict), 
                  rus.optional (rus.one_of (SYNC, ASYNC, TASK)))
    @rus.returns (rus.nothing)
>>>>>>> origin/titan
    def __init__ (self, url=None, flags=READ, session=None, 
                  _adaptor=None, _adaptor_state={}, _ttype=None) : 
        """
        __init__(url, flags=READ, session)

        Construct a new file object

        :param url:     Url of the (remote) file
        :type  url:     :class:`saga.Url` 

        :fgs:   :ref:`filesystemflags`
        :param session: :class:`saga.Session`

        The specified file is expected to exist -- otherwise a DoesNotExist
        exception is raised.  Also, the URL must point to a file (not to
        a directory), otherwise a BadParameter exception is raised.

        Example::

            # get a file handle
            file = saga.filesystem.File("sftp://localhost/tmp/data/data.bin")
    
            # print the file's size
            print file.get_size ()
        """

        # param checks
<<<<<<< HEAD
        url = surl.Url (url)

=======
        if  not flags : flags = 0
        url = surl.Url (url)

        if  not url.schema :
            url.schema = 'file'

        if  not url.host :
            url.host = 'localhost'

>>>>>>> origin/titan
        self._nsentry = super  (File, self)
        self._nsentry.__init__ (url, flags, session, 
                                _adaptor, _adaptor_state, _ttype=_ttype)

    # --------------------------------------------------------------------------
    #
    @classmethod
<<<<<<< HEAD
    @sus.takes   ('File', 
                  sus.optional ((surl.Url, basestring)), 
                  sus.optional (int), 
                  sus.optional (ss.Session),
                  sus.optional (sus.one_of (SYNC, ASYNC, TASK)))
    @sus.returns (st.Task)
=======
    @rus.takes   ('File', 
                  rus.optional ((surl.Url, basestring)), 
                  rus.optional (int, rus.nothing), 
                  rus.optional (ss.Session),
                  rus.optional (rus.one_of (SYNC, ASYNC, TASK)))
    @rus.returns (st.Task)
>>>>>>> origin/titan
    def create (cls, url=None, flags=READ, session=None, ttype=None) :
        """
        create(url, flags, session)

        url:       saga.Url
        flags:     saga.replica.flags enum
        session:   saga.Session
        ttype:     saga.task.type enum
        ret:       saga.Task
        """
<<<<<<< HEAD
=======
        if  not flags : flags = 0
>>>>>>> origin/titan
        _nsentry = super (File, cls)
        return _nsentry.create (url, flags, session, ttype=ttype)


    # ----------------------------------------------------------------
    #
    # filesystem methods
    #
    # --------------------------------------------------------------------------
    #
<<<<<<< HEAD
    @sus.takes   ('File', 
                  sus.optional (sus.one_of (SYNC, ASYNC, TASK)))
    @sus.returns ((bool, st.Task))
=======
    @rus.takes   ('File', 
                  rus.optional (rus.one_of (SYNC, ASYNC, TASK)))
    @rus.returns ((bool, st.Task))
>>>>>>> origin/titan
    def is_file (self, ttype=None) :
        """
        is_file()

        Returns `True` if instance points to a file, `False` otherwise. 
        """
        return self._adaptor.is_file_self (ttype=ttype)

  
    # --------------------------------------------------------------------------
    #
<<<<<<< HEAD
    @sus.takes   ('File', 
                  sus.optional (sus.one_of (SYNC, ASYNC, TASK)))
    @sus.returns ((int, st.Task))
=======
    @rus.takes   ('File', 
                  rus.optional (rus.one_of (SYNC, ASYNC, TASK)))
    @rus.returns ((int, st.Task))
>>>>>>> origin/titan
    def get_size (self, ttype=None) :
        '''
        get_size()
        
        Returns the size (in bytes) of a file.

           Example::

               # get a file handle
               file = saga.filesystem.File("sftp://localhost/tmp/data/data.bin")
    
               # print the file's size
               print file.get_size ()

        '''
        return self._adaptor.get_size_self (ttype=ttype)

  
    # --------------------------------------------------------------------------
    #
<<<<<<< HEAD
    @sus.takes   ('File', 
                  sus.optional (int),
                  sus.optional (sus.one_of (SYNC, ASYNC, TASK)))
    @sus.returns ((basestring, st.Task))
=======
    @rus.takes   ('File', 
                  rus.optional (int),
                  rus.optional (rus.one_of (SYNC, ASYNC, TASK)))
    @rus.returns ((basestring, st.Task))
>>>>>>> origin/titan
    def read     (self, size=None, ttype=None) :
        '''
        size :    int
        ttype:    saga.task.type enum
        ret:      string / bytearray / saga.Task
        '''
        return self._adaptor.read (size, ttype=ttype)

<<<<<<< HEAD
  
    # --------------------------------------------------------------------------
    #
    @sus.takes   ('File', 
                  basestring,
                  sus.optional (sus.one_of (SYNC, ASYNC, TASK)))
    @sus.returns ((int, st.Task))
=======
    # --------------------------------------------------------------------------
    #
    @rus.takes   ('File', 
                  rus.optional (bool))
    @rus.returns (st.Task)
    def close     (self, kill=True, ttype=None) :
        '''
        kill :    bool
        ttype:    saga.task.type enum
        ret:      string / bytearray / saga.Task
        '''
        return self._adaptor.close ()
  
    # --------------------------------------------------------------------------
    #
    @rus.takes   ('File', 
                  basestring,
                  rus.optional (rus.one_of (SYNC, ASYNC, TASK)))
    @rus.returns ((int, st.Task))
>>>>>>> origin/titan
    def write    (self, data, ttype=None) :
        '''
        data :    string / bytearray
        ttype:    saga.task.type enum
        ret:      int / saga.Task
        '''
        return self._adaptor.write (data, ttype=ttype)

  
    # --------------------------------------------------------------------------
    #
<<<<<<< HEAD
    @sus.takes   ('File', 
                  int,
                  sus.optional (sus.one_of (START, CURRENT, END )),
                  sus.optional (sus.one_of (SYNC,  ASYNC,   TASK)))
    @sus.returns ((int, st.Task))
=======
    @rus.takes   ('File', 
                  int,
                  rus.optional (rus.one_of (START, CURRENT, END )),
                  rus.optional (rus.one_of (SYNC,  ASYNC,   TASK)))
    @rus.returns ((int, st.Task))
>>>>>>> origin/titan
    def seek     (self, offset, whence=START, ttype=None) :
        '''
        offset:   int
        whence:   seek_mode enum
        ttype:    saga.task.type enum
        ret:      int / saga.Task
        '''
        return self._adaptor.seek (offset, whence, ttype=ttype)

  
    # --------------------------------------------------------------------------
    #
<<<<<<< HEAD
    @sus.takes   ('File', 
                  sus.list_of  (sus.tuple_of (int)),
                  sus.optional (sus.one_of (SYNC, ASYNC, TASK)))
    @sus.returns ((basestring, st.Task))
=======
    @rus.takes   ('File', 
                  rus.list_of  (rus.tuple_of (int)),
                  rus.optional (rus.one_of (SYNC, ASYNC, TASK)))
    @rus.returns ((basestring, st.Task))
>>>>>>> origin/titan
    def read_v   (self, iovecs, ttype=None) :
        '''
        iovecs:   list [tuple (int, int)]
        ttype:    saga.task.type enum
        ret:      list [bytearray] / saga.Task
        '''
        return self._adaptor.read_v (iovecs, ttype=ttype)

  
    # --------------------------------------------------------------------------
    #
<<<<<<< HEAD
    @sus.takes   ('File', 
                  sus.list_of  (sus.tuple_of ((int, basestring))),
                  sus.optional (sus.one_of (SYNC, ASYNC, TASK)))
    @sus.returns ((sus.list_of (int), st.Task))
=======
    @rus.takes   ('File', 
                  rus.list_of  (rus.tuple_of ((int, basestring))),
                  rus.optional (rus.one_of (SYNC, ASYNC, TASK)))
    @rus.returns ((rus.list_of (int), st.Task))
>>>>>>> origin/titan
    def write_v (self, data, ttype=None) :
        '''
        data:     list [tuple (int, string / bytearray)]
        ttype:    saga.task.type enum
        ret:      list [int] / saga.Task
        '''
        return self._adaptor.write_v (data, ttype=ttype)

  
    # --------------------------------------------------------------------------
    #
<<<<<<< HEAD
    @sus.takes   ('File', 
                  basestring,
                  sus.optional (sus.one_of (SYNC, ASYNC, TASK)))
    @sus.returns ((int, st.Task))
=======
    @rus.takes   ('File', 
                  basestring,
                  rus.optional (rus.one_of (SYNC, ASYNC, TASK)))
    @rus.returns ((int, st.Task))
>>>>>>> origin/titan
    def size_p (self, pattern, ttype=None) :
        '''
        pattern:  string 
        ttype:    saga.task.type enum
        ret:      int / saga.Task
        '''
        return self._adaptor.size_p (pattern, ttype=ttype)
  

    # --------------------------------------------------------------------------
    #
<<<<<<< HEAD
    @sus.takes   ('File', 
                  basestring,
                  sus.optional (sus.one_of (SYNC, ASYNC, TASK)))
    @sus.returns ((basestring, st.Task))
=======
    @rus.takes   ('File', 
                  basestring,
                  rus.optional (rus.one_of (SYNC, ASYNC, TASK)))
    @rus.returns ((basestring, st.Task))
>>>>>>> origin/titan
    def read_p (self, pattern, ttype=None) :
        '''
        pattern:  string
        ttype:    saga.task.type enum
        ret:      string / bytearray / saga.Task
        '''
        return self._adaptor.read_p (pattern, ttype=ttype)

  
    # --------------------------------------------------------------------------
    #
<<<<<<< HEAD
    @sus.takes   ('File', 
                  basestring,
                  basestring,
                  sus.optional (sus.one_of (SYNC, ASYNC, TASK)))
    @sus.returns ((int, st.Task))
=======
    @rus.takes   ('File', 
                  basestring,
                  basestring,
                  rus.optional (rus.one_of (SYNC, ASYNC, TASK)))
    @rus.returns ((int, st.Task))
>>>>>>> origin/titan
    def write_p (self, pattern, data, ttype=None) :
        '''
        pattern:  string
        data:     string / bytearray
        ttype:    saga.task.type enum
        ret:      int / saga.Task
        '''
        return self._adaptor.write_p (pattern, data, ttype=ttype)

  
    # --------------------------------------------------------------------------
    #
<<<<<<< HEAD
    @sus.takes   ('File', 
                  sus.optional (sus.one_of (SYNC, ASYNC, TASK)))
    @sus.returns ((sus.list_of (basestring), st.Task))
=======
    @rus.takes   ('File', 
                  rus.optional (rus.one_of (SYNC, ASYNC, TASK)))
    @rus.returns ((rus.list_of (basestring), st.Task))
>>>>>>> origin/titan
    def modes_e (self, ttype=None) :
        '''
        ttype:    saga.task.type enum
        ret:      list [string] / saga.Task
        '''
        return self._adaptor.modes_e (ttype=ttype)

  
    # --------------------------------------------------------------------------
    #
<<<<<<< HEAD
    @sus.takes   ('File', 
                  basestring,
                  basestring,
                  sus.optional (sus.one_of (SYNC, ASYNC, TASK)))
    @sus.returns ((int, st.Task))
=======
    @rus.takes   ('File', 
                  basestring,
                  basestring,
                  rus.optional (rus.one_of (SYNC, ASYNC, TASK)))
    @rus.returns ((int, st.Task))
>>>>>>> origin/titan
    def size_e (self, emode, spec, ttype=None) :
        '''
        emode:    string
        spec:     string
        ttype:    saga.task.type enum
        ret:      int / saga.Task
        '''
        return self._adaptor.size_e (emode, spec, ttype=ttype)

  
    # --------------------------------------------------------------------------
    #
<<<<<<< HEAD
    @sus.takes   ('File', 
                  basestring,
                  basestring,
                  sus.optional (sus.one_of (SYNC, ASYNC, TASK)))
    @sus.returns ((basestring, st.Task))
=======
    @rus.takes   ('File', 
                  basestring,
                  basestring,
                  rus.optional (rus.one_of (SYNC, ASYNC, TASK)))
    @rus.returns ((basestring, st.Task))
>>>>>>> origin/titan
    def read_e (self, emode, spec, ttype=None) :
        '''
        emode:    string
        spec:     string
        ttype:    saga.task.type enum
        ret:      bytearray / saga.Task
        '''
        return self._adaptor.read_e (emode, spec, ttype=ttype)

  
    # --------------------------------------------------------------------------
    #
<<<<<<< HEAD
    @sus.takes   ('File', 
                  basestring,
                  basestring,
                  basestring,
                  sus.optional (sus.one_of (SYNC, ASYNC, TASK)))
    @sus.returns ((int, st.Task))
=======
    @rus.takes   ('File', 
                  basestring,
                  basestring,
                  basestring,
                  rus.optional (rus.one_of (SYNC, ASYNC, TASK)))
    @rus.returns ((int, st.Task))
>>>>>>> origin/titan
    def write_e (self, emode, spec, data, ttype=None) :
        '''
        emode:    string
        spec:     string
        data:     string / bytearray
        ttype:    saga.task.type enum
        ret:      int / saga.Task
        '''
        return self._adaptor.read_e (emode, spec, data, ttype=ttype)

  
    size    = property (get_size)  # int
    modes_e = property (modes_e)   # list [string]
  
  
<<<<<<< HEAD
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4
=======

>>>>>>> origin/titan

