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

from .mover import JobMover

