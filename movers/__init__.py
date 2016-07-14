"""
  Site Movers package
  :author: Alexey Anisenkov
"""

from .base import BaseSiteMover

# import evertying here to allow import movers explicitly by its name from the package
from .sitemovers import *

import sitemovers


def getSiteMover(name):
    """ Resolve Site Mover class by its ID name """

    # get all mover classes
    mclasses = dict([getattr(sitemovers, key).getID(), getattr(sitemovers, key)] for key in dir(sitemovers) if not key.startswith('_') \
                and issubclass(getattr(sitemovers, key), BaseSiteMover))

    # resolve mover by name
    mover = mclasses.get(name)
    if not mover:
        raise ValueError('SiteMoverFactory: Failed to resolve site mover by name="%s": NOT IMPLEMENTED, accepted_names=%s' % (name, sorted(mclasses)))

    return mover

def getSiteMoverByScheme(scheme, allowed_movers=None):
    """
        Resolve Site Mover class by protocol scheme
        Default (hard-coded) map
    """

    # list of movers supported for following scheme protocols
    cmap = {
            #'scheme': [ordered list of preferred movers by DEFAULT] -- site can restrict the list of preferred movers by passing allowed_movers variable
            'root': ['xrdcp', 'rucio', 'lcgcp', 'lsm'],
            'srm': ['rucio', 'lcgcp', 'lsm'],
            'dcap': ['dccp'],
            #'gsiftp': ['rucio'],
            #'https': ['rucio'],
            # default sitemover to be used
            #'default': ['rucio']
    }

    dat = cmap.get(scheme) or cmap.get('default', [])
    for mover in dat:
        if allowed_movers and mover not in allowed_movers:
            continue
        return getSiteMover(mover)

    return None

    raise ValueError('SiteMoverFactory/getSiteMoverByScheme: Failed to resolve site mover for scheme="%s", allowed_movers=%s .. associated_movers=%s' % (scheme, allowed_movers, dat))


from .mover import JobMover
