

import os
import regex
import url as ruu


# ------------------------------------------------------------------------------
#
def split_dburl (dburl, default_dburl=None) :
    """
    we split the url into the base mongodb URL, and the path element, whose
    first element is the database name, and the remainder is interpreted as
    collection id.
    """

    # if the given URL does not contain schema nor host, the default URL is used
    # as base, and the given URL string is appended to the path element.
    
    url = ruu.Url (dburl)

    if  not url.schema and not url.host :
        url      = ruu.Url (default_dburl)
        url.path = dburl

    # NOTE: add other data base schemes here...
    if  url.schema not in ['mongodb'] :
        raise ValueError ("url must be a 'mongodb://' url, not %s" % dburl)

    host = url.host
    port = url.port
    path = url.path
    user = url.username
    pwd  = url.password

    if  path.startswith ('/') :
        path = path[1:]
    path_elems = path.split ('/')

    dbname = None
    cname  = None
    pname  = None

    if  len(path_elems)  >  0 :
        dbname = path_elems[0]

    if  len(path_elems)  >  1 :
        dbname = path_elems[0]
        cname  = path_elems[1]

    if  len(path_elems)  >  2 :
        dbname = path_elems[0]
        cname  = path_elems[1]
        pname  = '/'.join (path_elems[2:])

    if  dbname == '.' : 
        dbname = None

    return [host, port, dbname, cname, pname, user, pwd]


# ------------------------------------------------------------------------------
#
def mongodb_connect (dburl, default_dburl=None) :
    """
    connect to the given mongodb, perform auth for the database (if a database
    was given).
    """

    try :
        import pymongo
    except ImportError :
        msg  = " \n\npymongo is not available -- install radical.utils with: \n\n"
        msg += "  (1) pip install --upgrade -e '.[pymongo]'\n"
        msg += "  (2) pip install --upgrade    'radical.utils[pymongo]'\n\n"
        msg += "to resolve that dependency (or install pymongo manually).\n"
        msg += "The first version will work for local installation, \n"
        msg += "the second one for installation from pypi.\n\n"
        raise ImportError (msg)

    [host, port, dbname, cname, pname, user, pwd] = split_dburl (dburl, default_dburl)

    mongo = pymongo.MongoClient (host=host, port=port)
    db    = None

    if  dbname :
        db = mongo[dbname]

        if  user and pwd :
            db.authenticate (user, pwd)


    else :

        # if no DB is given, we try to auth against all databases.
        for dbname in mongo.database_names () :
            try :
                mongo[dbname].authenticate (user, pwd)
            except Exception as e :
                pass 


    return mongo, db, dbname, cname, pname


# ------------------------------------------------------------------------------
#
def parse_file_staging_directives (directives) :
    """
    staging directives

       [local_path] [operator] [remote_path]

    local path: 
        * interpreted as relative to the application's working directory
        * must point to local storage (localhost)
    
    remote path
        * interpreted as relative to the job's working directory

    operator :
        * >  : stage to remote target, overwrite if exists
        * >> : stage to remote target, append    if exists
        * <  : stage to local  target, overwrite if exists
        * << : stage to local  target, append    if exists

    This method returns a tuple [src, tgt, op] for each given directive.  This
    parsing is backward compatible with the simple staging directives used
    previously -- any strings which do not contain staging operators will be
    interpreted as simple paths (identical for src and tgt), operation is set to
    '=', which must be interpreted in the caller context.  
    """

    bulk = True
    if  not isinstance (directives, list) :
        bulk       = False
        directives = [directives]

    ret = list()

    for directive in directives :

        if  not isinstance (directive, basestring) :
            raise TypeError ("file staging directives muct by of type string, "
                             "not %s" % type(directive))

        rs = regex.ReString (directive)

        if  rs // '^(?P<one>.+?)\s*(?P<op><|<<|>|>>)\s*(?P<two>.+)$' :
            res = rs.get ()
            ret.append ([res['one'], res['two'], res['op']])

        else :
            ret.append ([directive, directive, '='])

    if  bulk : return ret
    else     : return ret[0]


# ------------------------------------------------------------------------------
#
def time_diff (dt_abs, dt_stamp) :
    """
    return the time difference bewteen  two datetime 
    objects in seconds (incl. fractions).  Exceptions (like on improper data
    types) fall through.
    """

    delta = dt_stamp - dt_abs

    import datetime
    if  not isinstance  (delta, datetime.timedelta) :
        raise TypeError ("difference between '%s' and '%s' is not a .timedelta" \
                      % (type(dt_abs), type(dt_stamp)))

    # get seconds as float 
    seconds = delta.seconds + delta.microseconds/1E6
    return seconds


# ------------------------------------------------------------------------------
#
def _get_stacktraces () :

    import sys
    import threading
    import traceback

    id2name = {}
    for th in threading.enumerate():
        id2name[th.ident] = th.name

    code = []
    for threadId, stack in sys._current_frames().items():
        code.append("\n# Thread: %s(%d)" % (id2name[threadId], threadId))

        for filename, lineno, name, line in traceback.extract_stack(stack):
            code.append('File: "%s", line %d, in %s' % (filename, lineno, name))

            if line:
                code.append(" %s" % (line.strip()))

    return "\n".join(code)


# ------------------------------------------------------------------------------
#
class DebugHelper (object) :
    """
    When instantiated, and when "RADICAL_DEBUG" is set in the environmant, this
    class will install a signal handler for SIGINFO.  When that signal is
    received, a stacktrace for all threads is printed to stdout.  Note that 
    <CTRL-T> also triggers that signal on the terminal.
    """

    def __init__ (self) :
        import os

        if 'RADICAL_DEBUG' in os.environ :
            import signal
            signal.signal(signal.SIGINFO, self.dump_stacktraces)


    def dump_stacktraces (self, a, b) :
        print _get_stacktraces ()


# ------------------------------------------------------------------------------
#
def all_pairs (iterable, n) :
    """
    s -> (s0,s1,s2,...sn-1), (sn,sn+1,sn+2,...s2n-1), (s2n,s2n+1,s2n+2,...s3n-1), ...
    """

    from itertools import izip
    return izip(*[iter(iterable)]*n)


# ------------------------------------------------------------------------------
# From https://docs.python.org/release/2.3.5/lib/itertools-example.html
#
def window (seq, n=2) :
    """
    Returns a sliding window (of width n) over data from the iterable"
    s -> (s0,s1,...s[n-1]), (s1,s2,...,sn), ... 
    """

    from itertools import islice

    it = iter(seq)
    result = tuple(islice(it, n))

    if len(result) == n :
        yield result

    for elem in it :
        result = result[1:] + (elem,)
        yield result

# ------------------------------------------------------------------------------


