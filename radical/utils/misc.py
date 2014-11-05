

import os
import regex
#import pymongo
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

'''
# ------------------------------------------------------------------------------
#
def mongodb_connect (dburl, default_dburl=None) :
    """
    connect to the given mongodb, perform auth for the database (if a database
    was given).
    """

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
'''

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

